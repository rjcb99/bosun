package expr

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2018-03-01/insights"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2018-02-01/resources"
	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/kylebrandt/boolq"
)

// Functions for Querying Azure Montior
var AzureMonitor = map[string]parse.Func{
	"az": {
		Args:          []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet,
		Tags:          tagFirst, //TODO: Appropriate tags func
		F:             AzureQuery,
		PrefixEnabled: true,
	},
	"azmulti": {
		Args:          []models.FuncType{models.TypeString, models.TypeString, models.TypeAzureResourceList, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet,
		Tags:          tagFirst, //TODO: Appropriate tags func
		F:             AzureMultiQuery,
		PrefixEnabled: true,
	},
	"azmd": {
		Args:          []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet, // TODO return type
		Tags:          tagFirst,             //TODO: Appropriate tags func
		F:             AzureMetricDefinitions,
		PrefixEnabled: true,
	},
	"azrt": {
		Args:          []models.FuncType{models.TypeString},
		Return:        models.TypeAzureResourceList,
		F:             AzureResourcesByType,
		PrefixEnabled: true,
	},
	"azrf": {
		Args:   []models.FuncType{models.TypeAzureResourceList, models.TypeString},
		Return: models.TypeAzureResourceList,
		F:      AzureFilterResources,
	},
}

// Azure API References
// - https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-supported-metrics
// - https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-data-sources

// TODO Aggregation types?
// - I'm not sure all aggregations are available for all metrics, need to explore

// TODO Auto timegrain/interval: Func that decides the timegrain based on the duration of the span of time between start and end

// TODO Cache
// - Used different Cache for resource list
// - cache metric md?

const azTimeFmt = "2006-01-02T15:04:05"

func azResourceURI(subscription, resourceGrp, Namespace, Resource string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s", subscription, resourceGrp, Namespace, Resource)
}

func AzureMetricDefinitions(prefix string, e *State, T miniprofiler.Timer, namespace, metric, rsg, resource string) (r *Results, err error) {
	r = new(Results)
	cc, clientFound := e.Backends.AzureMonitor[prefix]
	if !clientFound {
		return r, fmt.Errorf("azure client with name %v not defined", prefix)
	}
	c := cc.MetricDefinitionsClient
	// TODO fix context
	ctx := context.Background()
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
func AzureQuery(prefix string, e *State, T miniprofiler.Timer, namespace, metric, tagKeysCSV, rsg, resource, agtype, interval, sdur, edur string) (r *Results, err error) {
	r = new(Results)
	cc, clientFound := e.Backends.AzureMonitor[prefix]
	if !clientFound {
		return r, fmt.Errorf("azure client with name %v not defined", prefix)
	}
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
	st := e.now.Add(time.Duration(-sd)).Format(azTimeFmt)
	en := e.now.Add(time.Duration(-ed)).Format(azTimeFmt)

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
	c := cc.MetricsClient
	cacheKey := strings.Join([]string{prefix, namespace, metric, tagKeysCSV, rsg, resource, agtype, interval, st, en}, ":")
	getFn := func() (interface{}, error) {
		resp, err := c.List(ctx, azResourceURI(c.SubscriptionID, rsg, namespace, resource),
			fmt.Sprintf("%s/%s", st, en),
			tg,
			metric,
			aggLong,
			nil,
			"asc",
			filter,
			insights.Data,
			namespace)
		if err != nil {
			return resp, err
		}
		return resp, nil
	}
	val, err := e.Cache.Get(cacheKey, getFn)
	if err != nil {
		return r, err
	}
	resp := val.(insights.Response)
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
				tags["rsg"] = rsg
				tags["name"] = resource
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
				if len(series) == 0 {
					continue
				}
				r.Results = append(r.Results, &Result{
					Value: series,
					Group: tags,
				})
			}

		}
	}
	return r, nil
}

// $resources = azrt("Microsoft.Compute/virtualMachines")
// azmulti("Percentage CPU", "", $resources, "max", "PT5M", "1h", "")
func AzureMultiQuery(prefix string, e *State, T miniprofiler.Timer, metric, tagKeysCSV string, resources AzureResources, agtype string, interval, sdur, edur string) (r *Results, err error) {
	r = new(Results)
	queryResults := []*Results{}
	// TODO: Since each of these is an http query, should run N queries parallel from a pool or something like this
	for _, resource := range resources {
		res, err := AzureQuery(prefix, e, T, resource.Type, metric, tagKeysCSV, resource.ResourceGroup, resource.Name, agtype, interval, sdur, edur)
		if err != nil {
			return r, err
		}
		queryResults = append(queryResults, res)
	}
	r, err = Merge(e, T, queryResults...)
	return
}

func AzureListResources(prefix string, e *State, T miniprofiler.Timer) (AzureResources, error) {
	// TODO Make cache time configurable
	// TODO Possibly use a different additional cache for this - not shared with queries?
	key := fmt.Sprintf("AzureResourceCache:%s:%s", prefix, time.Now().Truncate(time.Minute*1)) // https://github.com/golang/groupcache/issues/92
	getFn := func() (interface{}, error) {
		r := AzureResources{}
		cc, clientFound := e.Backends.AzureMonitor[prefix]
		if !clientFound {
			return r, fmt.Errorf("azure client with name %v not defined", prefix)
		}
		c := cc.ResourcesClient
		ctx := context.Background() // TODO fix
		for rList, err := c.ListComplete(ctx, "", "", nil); rList.NotDone(); err = rList.Next() {
			// TODO not catching auth error here for some reason, err is nil when error!!
			if err != nil {
				return r, err
			}
			val := rList.Value()
			if val.Name != nil && val.Type != nil && val.ID != nil {
				splitID := strings.Split(*val.ID, "/")
				if len(splitID) < 5 {
					return r, fmt.Errorf("unexpected ID for resource: %s", *val.ID)
				}
				azTags := make(map[string]string)
				for k, v := range val.Tags {
					if v != nil {
						azTags[k] = *v
					}
				}
				r = append(r, AzureResource{
					Name:          *val.Name,
					Type:          *val.Type,
					ResourceGroup: splitID[4],
					Tags:          azTags,
				})
			}
		}
		return r, nil
	}
	val, err := e.Cache.Get(key, getFn)
	if err != nil {
		return AzureResources{}, err
	}
	return val.(AzureResources), nil
}

func AzureResourcesByType(prefix string, e *State, T miniprofiler.Timer, tp string) (r *Results, err error) {
	resources := AzureResources{}
	r = new(Results)
	allResources, err := AzureListResources(prefix, e, T)
	if err != nil {
		return
	}
	for _, res := range allResources {
		if res.Type == tp {
			resources = append(resources, res)
		}
	}
	r.Results = append(r.Results, &Result{Value: resources})
	return
}

func AzureFilterResources(e *State, T miniprofiler.Timer, resources AzureResources, filter string) (r *Results, err error) {
	r = new(Results)
	bqf, err := boolq.Parse(filter)
	if err != nil {
		return r, err
	}
	filteredResources := AzureResources{}
	for _, res := range resources {
		match, err := boolq.AskParsedExpr(bqf, res)
		if err != nil {
			return r, err
		}
		if match {
			filteredResources = append(filteredResources, res)
		}
	}
	r.Results = append(r.Results, &Result{Value: filteredResources})
	return
}

type AzureResource struct {
	Name          string
	Type          string
	ResourceGroup string
	Tags          map[string]string
}

type AzureResources []AzureResource

func (ar AzureResource) Ask(filter string) (bool, error) {
	sp := strings.SplitN(filter, ":", 2)
	if len(sp) != 2 {
		return false, fmt.Errorf("bad filter, filter must be in k:v format, got %v", filter)
	}
	key := strings.ToLower(sp[0]) // Make key case insensitive
	value := sp[1]
	switch key {
	case "name":
		re, err := regexp.Compile(value)
		if err != nil {
			return false, err
		}
		if re.MatchString(ar.Name) {
			return true, nil
		}
	case "rsg", "resourcegroup":
		re, err := regexp.Compile(value)
		if err != nil {
			return false, err
		}
		if re.MatchString(ar.ResourceGroup) {
			return true, nil
		}
	default: // Does not support tags that have a tag key of rsg, resourcegroup, or name
		if tagV, ok := ar.Tags[key]; ok {
			re, err := regexp.Compile(value)
			if err != nil {
				return false, err
			}
			if re.MatchString(tagV) {
				return true, nil
			}
		}

	}
	return false, nil
}

type AzureMonitorClientCollection struct {
	MetricsClient           insights.MetricsClient
	MetricDefinitionsClient insights.MetricDefinitionsClient
	ResourcesClient         resources.Client
}

type AzureMonitorClients map[string]AzureMonitorClientCollection

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
