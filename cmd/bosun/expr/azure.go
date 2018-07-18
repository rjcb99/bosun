package expr

import (
	"context"
	"fmt"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2018-03-01/insights"
	"github.com/MiniProfiler/go/miniprofiler"
)

// Functions for Querying Azure Montior
var AzureMonitor = map[string]parse.Func{
	"az": {
		Args:   []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return: models.TypeSeriesSet,
		Tags:   tagFirst,
		F:      AzureQuery,
	},
}

func AzureQuery(e *State, T miniprofiler.Timer, metric, rsg, namespace, resource string) (r *Results, err error) {
	c := e.Backends.AzureMonitor
	// TODO fix context
	ctx := context.Background()
	fmt.Println(c.SubscriptionID)
	//tg := "PT1M"
	uri := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s", c.SubscriptionID, rsg, namespace, resource)
	z, err := c.List(ctx, uri,
		"2018-07-17T19:15:58Z/2018-07-17T20:15:58Z",
		nil,
		metric,
		"Average",
		nil,
		"asc",
		"",
		insights.Data,
		"Microsoft.Compute/virtualMachines")
	if err != nil {
		return nil, err
	}
	if z.Value != nil {
		for _, x := range *z.Value {
			if x.Timeseries == nil {
				continue
			}
			for _, y := range *x.Timeseries {
				if y.Data == nil {
					continue
				}
				for _, w := range *y.Data {
					fmt.Println(w.TimeStamp, *w.Average)
				}
			}
		}
	}
	return nil, nil
}
