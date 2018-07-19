package expr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/Azure/azure-sdk-for-go/profiles/latest/resources/mgmt/resources"
	"github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2018-03-01/insights"
	"github.com/MiniProfiler/go/miniprofiler"
)

// Functions for Querying Azure Montior
var AzureMonitor = map[string]parse.Func{
	"az": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   tagFirst, //TODO: Appropriate tags func
		F:      AzureQuery,
	},
	"azmd": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet, // TODO return type
		Tags:   tagFirst,             //TODO: Appropriate tags func
		F:      AzureMetricDefinitions,
	},
	"azr": {
		Args:   []models.FuncType{},
		Return: models.TypeSeriesSet, //TODO return type
		Tags:   tagFirst,             //TODO: Appropriate tags func
		F:      AzureListResources,
	},
}

// Reference for supported metrics: https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-supported-metrics

// TODO Handling multiple resources
// - Given a metric and resource group, get the values for each object of ... the same type? and then tag them as such
// - Like the above, but gets all resources groups and all the objects in each resource group

// TODO Aggregation types?
// - I'm not sure all aggregations are available for all metrics, need to explore

// TODO Auto timegrain/interval: Func that decides the timegrain based on the duration of the span of time between start and end

// TODO Cache
// - Cache for time series queries
// - Cache for MetricDefintion data - probably longer lived

const azTimeFmt = "2006-01-02T15:04:05"

func azResourceURI(subscription, resourceGrp, Namespace, Resource string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s", subscription, resourceGrp, Namespace, Resource)
}

func AzureMetricDefinitions(e *State, T miniprofiler.Timer, namespace, metric, rsg, resource string) (r *Results, err error) {
	c := e.Backends.AzureMonitor.MetricDefinitionsClient
	// TODO fix context
	ctx := context.Background()
	r = new(Results)

	defs, err := c.List(ctx, azResourceURI(c.SubscriptionID, rsg, namespace, resource), namespace)
	if err != nil {
		return
	}
	if defs.Value == nil {
		return r, fmt.Errorf("No metric definitions in response")
	}
	for _, def := range *defs.Value {
		agtypes := []string{}
		for _, x := range *def.SupportedAggregationTypes {
			agtypes = append(agtypes, fmt.Sprintf("%s", x))
		}
		dims := []string{}
		if def.Dimensions != nil {
			for _, x := range *def.Dimensions {
				dims = append(dims, fmt.Sprintf("%s", *x.Value))
			}
		}
		fmt.Println(*def.Name.LocalizedValue, strings.Join(dims, ", "), strings.Join(agtypes, ", "))

	}
	return
}

// az("Microsoft.Compute/virtualMachines", "Percentage CPU", "SRE-RSG", "SRE-Linux-Jump", "avg" "PT5M", "1h", "")
// az("Microsoft.Compute/virtualMachines", "Per Disk Read Bytes/sec", "SlotId", "SRE-RSG", "SRE-Linux-Jump", "max", "PT5M", "1h", "")
func AzureQuery(e *State, T miniprofiler.Timer, namespace, metric, tagKeysCSV, rsg, resource, agtype, interval, sdur, edur string) (r *Results, err error) {
	c := e.Backends.AzureMonitor.MetricsClient
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

	filter := ""
	if tagKeysCSV != "" {
		filters := []string{}
		tagKeys := strings.Split(tagKeysCSV, ",")
		for _, k := range tagKeys {
			filters = append(filters, fmt.Sprintf("%s eq '*'", k))
		}
		filter = strings.Join(filters, " and ")
	}
	var tg *string
	if interval != "" {
		tg = &interval
	}

	aggLong, err := AzureShortAggToLong(agtype)
	if err != nil {
		return
	}

	resp, err := c.List(ctx, azResourceURI(c.SubscriptionID, rsg, namespace, resource),
		fmt.Sprintf("%s/%s", st.Format(azTimeFmt), en.Format(azTimeFmt)),
		tg,
		metric,
		aggLong,
		nil,
		"asc",
		filter,
		insights.Data,
		namespace)
	if err != nil {
		return
	}
	// Optional todo capture X-Ms-Ratelimit-Remaining-Subscription-Reads
	if resp.Value != nil {
		for _, tsContainer := range *resp.Value {
			if tsContainer.Timeseries == nil {
				continue
			}
			for _, dataContainer := range *tsContainer.Timeseries {
				if dataContainer.Data == nil {
					continue
				}
				series := make(Series)
				tags := make(opentsdb.TagSet)
				if dataContainer.Metadatavalues != nil {
					for _, md := range *dataContainer.Metadatavalues {
						if md.Name != nil && md.Name.Value != nil && md.Value != nil {
							tags[*md.Name.Value] = *md.Value
						} // TODO: Else?
					}
				}
				for _, mValue := range *dataContainer.Data {
					exValue := AzureExtractMetricValue(&mValue, aggLong)
					if exValue != nil && mValue.TimeStamp != nil {
						series[mValue.TimeStamp.ToTime()] = *exValue
					}
				}
				r.Results = append(r.Results, &Result{
					Value: series,
					Group: tags,
				})
			}

		}
	}

	return
}

func AzureListResources(e *State, T miniprofiler.Timer) (r *Results, err error) {
	c := e.AzureMonitor.ResourcesClient
	ctx := context.Background() // TODO fix
	r = new(Results)
	resources := []AzureResource{}
	for rList, err := c.ListComplete(ctx, "", "", nil); rList.NotDone(); err = rList.Next() {
		if err != nil {
			return r, err
		}
		val := rList.Value()
		if val.Name != nil && val.Type != nil && val.ID != nil {
			splitID := strings.Split(*val.ID, "/")
			if len(splitID) < 5 {
				return r, fmt.Errorf("unexpected ID for resource: %s", *val.ID)
			}
			resources = append(resources, AzureResource{
				Name:          *val.Name,
				Type:          *val.Type,
				ResourceGroup: splitID[4],
			})
		}
	}
	for _, r := range resources {
		fmt.Println(r)
	}
	return
}

type AzureResource struct {
	Name          string
	Type          string
	ResourceGroup string
}

type AzureMonitorClients struct {
	MetricsClient           insights.MetricsClient
	MetricDefinitionsClient insights.MetricDefinitionsClient
	ResourcesClient         resources.Client
}

func AzureExtractMetricValue(mv *insights.MetricValue, field string) (v *float64) {
	switch field {
	case string(insights.Average), "":
		v = mv.Average
	case string(insights.Minimum):
		v = mv.Minimum
	case string(insights.Maximum):
		v = mv.Maximum
	case string(insights.Total):
		v = mv.Total
	}
	return
}

func AzureShortAggToLong(agtype string) (string, error) {
	switch agtype {
	case "avg", "":
		return string(insights.Average), nil
	case "min":
		return string(insights.Minimum), nil
	case "max":
		return string(insights.Maximum), nil
	case "total":
		return string(insights.Total), nil
	}
	return "", fmt.Errorf("unrecognized aggregation type %s, must be avg, min, max, or total", agtype)
}
