package expr

import (
	"context"
	"fmt"
	"time"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2018-03-01/insights"
	"github.com/MiniProfiler/go/miniprofiler"
)

// Functions for Querying Azure Montior
var AzureMonitor = map[string]parse.Func{
	"az": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   tagFirst,
		F:      AzureQuery,
	},
}

// Reference for supported metrics: https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-supported-metrics

// TODO handling multi-dimensional metrics?
// - Should be in metadata field, maybe need to make a thing that gets metric defintions and caches them so we now what the
//    tags keys are before getting results ...

// TODO Handling multiple resources
// - Given a metric and resource group, get the values for each object of ... the same type? and then tag them as such
// - Like the above, but gets all resources groups and all the objects in each resource group

// TODO Aggregation types?
// - I'm not sure all aggregations are available for all metrics, need to explore

// TODO Timegrain options

const azTimeFmt = "2006-01-02T15:04:05"

// az("Microsoft.Compute/virtualMachines", "Percentage CPU", "SRE-RSG", "SRE-Linux-Jump", "25m", "")
func AzureQuery(e *State, T miniprofiler.Timer, namespace, metric, rsg, resource, sdur, edur string) (r *Results, err error) {
	c := e.Backends.AzureMonitor
	// TODO fix context
	ctx := context.Background()
	r = new(Results)
	// Parse Relative Time to absolute time
	sd, err := opentsdb.ParseDuration(sdur)
	if err != nil {
		return
	}
	var ed opentsdb.Duration
	if edur != "" {
		ed, err = opentsdb.ParseDuration(edur)
		if err != nil {
			return
		}
	}
	st := e.now.Add(time.Duration(-sd))
	en := e.now.Add(time.Duration(-ed))

	//tg := "PT1M"
	uri := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s", c.SubscriptionID, rsg, namespace, resource)
	z, err := c.List(ctx, uri,
		fmt.Sprintf("%s/%s", st.Format(azTimeFmt), en.Format(azTimeFmt)),
		nil,
		metric,
		"Average",
		nil,
		"asc",
		"",
		insights.Data,
		namespace)
	if err != nil {
		return nil, err
	}
	// Optional todo capture X-Ms-Ratelimit-Remaining-Subscription-Reads
	if z.Value != nil {
		for _, val := range *z.Value {
			if val.Timeseries == nil {
				continue
			}
			series := make(Series)
			for _, y := range *val.Timeseries {
				if y.Data == nil {
					continue
				}
				for _, w := range *y.Data {
					if w.Average != nil {
						series[w.TimeStamp.ToTime()] = *w.Average
					}
				}
			}
			tags := make(opentsdb.TagSet)
			r.Results = append(r.Results, &Result{
				Value: series,
				Group: tags,
			})
		}
	}

	return
}
