package expr

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"bosun.org/slog"

	"bosun.org/cmd/bosun/expr/parse"
	"bosun.org/models"
	"bosun.org/opentsdb"
	"github.com/Azure/azure-sdk-for-go/services/preview/monitor/mgmt/2018-03-01/insights"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2018-02-01/resources"
	"github.com/MiniProfiler/go/miniprofiler"
	"github.com/kylebrandt/boolq"
)

// AzureMonitor is the collection of functions for the azure monitor datasource
var AzureMonitor = map[string]parse.Func{
	"az": {
		Args:          []models.FuncType{models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet,
		Tags:          azTags,
		F:             AzureQuery,
		PrefixEnabled: true,
	},
	"azmulti": {
		Args:          []models.FuncType{models.TypeString, models.TypeString, models.TypeAzureResourceList, models.TypeString, models.TypeString, models.TypeString, models.TypeString},
		Return:        models.TypeSeriesSet,
		Tags:          azMultiTags,
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

// Tag function for the "az" expression function
func azTags(args []parse.Node) (parse.Tags, error) {
	return azureTags(args[2])
}

// Tag function for the "azmulti" expression function
func azMultiTags(args []parse.Node) (parse.Tags, error) {
	return azureTags(args[1])
}

// azureTags adds tags for the csv argument along with the "name" and "rsg" tags
func azureTags(arg parse.Node) (parse.Tags, error) {
	tags := parse.Tags{"name": struct{}{}, "rsg": struct{}{}}
	csvTags := strings.Split(arg.(*parse.StringNode).Text, ",")
	for _, k := range csvTags {
		tags[k] = struct{}{}
	}
	return tags, nil
}

// Azure API References
// - https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-supported-metrics
// - https://docs.microsoft.com/en-us/azure/monitoring-and-diagnostics/monitoring-data-sources

// TODO Cache
// - Used different Cache for resource list
// - cache metric md?

const azTimeFmt = "2006-01-02T15:04:05"

func azResourceURI(subscription, resourceGrp, Namespace, Resource string) string {
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/%s/%s", subscription, resourceGrp, Namespace, Resource)
}

// AzureMetricDefinitions fetches metric information for a specific resource and metric tuple
// TODO make this return and not fmt.Printf
func AzureMetricDefinitions(prefix string, e *State, T miniprofiler.Timer, namespace, metric, rsg, resource string) (r *Results, err error) {
	r = new(Results)
	cc, clientFound := e.Backends.AzureMonitor[prefix]
	if !clientFound {
		return r, fmt.Errorf("azure client with name %v not defined", prefix)
	}
	c := cc.MetricDefinitionsClient
	defs, err := c.List(context.Background(), azResourceURI(c.SubscriptionID, rsg, namespace, resource), namespace)
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

// AzureQuery queries an Azure monitor metric for the given resource and returns a series set tagged by
func AzureQuery(prefix string, e *State, T miniprofiler.Timer, namespace, metric, tagKeysCSV, rsg, resName, agtype, interval, sdur, edur string) (r *Results, err error) {
	r = new(Results)
	// Verify prefix is a defined resource and fetch the collection of clients
	cc, clientFound := e.Backends.AzureMonitor[prefix]
	if !clientFound {
		return r, fmt.Errorf("azure client with name %v not defined", prefix)
	}
	c := cc.MetricsClient
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

	// Set Dimensions (tag) keys for metrics that support them by building a filter
	filter := ""
	if tagKeysCSV != "" {
		filters := []string{}
		tagKeys := strings.Split(tagKeysCSV, ",")
		for _, k := range tagKeys {
			filters = append(filters, fmt.Sprintf("%s eq '*'", k))
		}
		filter = strings.Join(filters, " and ")
	}

	// Set the Interval/Timegrain (Azure metric downsampling)
	var tg *string
	if interval != "" {
		tg = &interval
	}

	// Set azure aggregation method
	aggLong, err := AzureShortAggToLong(agtype)
	if err != nil {
		return
	}
	cacheKey := strings.Join([]string{prefix, namespace, metric, tagKeysCSV, rsg, resName, agtype, interval, st, en}, ":")
	// Function to fetch Azure Metric values
	getFn := func() (interface{}, error) {
		req, err := c.ListPreparer(context.Background(), azResourceURI(c.SubscriptionID, rsg, namespace, resName),
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
			return nil, err
		}
		var resp insights.Response
		T.StepCustomTiming("azure", "query", req.URL.String(), func() {
			hr, sendErr := c.ListSender(req)
			if sendErr == nil {
				resp, err = c.ListResponder(hr)
			} else {
				err = sendErr
			}
		})
		return resp, err
	}
	// Get azure metric values by calling azure or via cache if available
	val, err := e.Cache.Get(cacheKey, getFn)
	if err != nil {
		return r, err
	}
	resp := val.(insights.Response)
	rawReadsRemaining := resp.Header.Get("X-Ms-Ratelimit-Remaining-Subscription-Reads")
	readsRemaining, err := strconv.ParseInt(rawReadsRemaining, 10, 64)
	if err != nil {
		slog.Errorf("failure to parse remaning reads from azure response")
	}
	if err == nil && readsRemaining < 100 {
		slog.Warningf("less than 100 reads detected for the azure api on client %v", prefix)
	}
	if resp.Value != nil {
		for _, tsContainer := range *resp.Value {
			if tsContainer.Timeseries == nil {
				continue // If the container doesn't have a time series object then skip
			}
			for _, dataContainer := range *tsContainer.Timeseries {
				if dataContainer.Data == nil {
					continue // The timeseries has no data in it - then skip
				}
				series := make(Series)
				tags := make(opentsdb.TagSet)
				tags["rsg"] = rsg
				tags["name"] = resName
				// Get the Key/Values that make up the azure dimension and turn them into tags
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
					continue // If we end up with an empty series then skip
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

// AzureMultiQuery queries multiple Azure resources and returns them as a single result set
// It makes one HTTP request per resource and parallelizes the requests
func AzureMultiQuery(prefix string, e *State, T miniprofiler.Timer, metric, tagKeysCSV string, resources AzureResources, agtype string, interval, sdur, edur string) (r *Results, err error) {
	r = new(Results)
	queryResults := []*Results{}

	workerConcurrency := 5 // TODO take this from configuration
	var wg sync.WaitGroup
	// reqCh (Request Channel) is populated with azure resources, and resources are pulled from channel to make a time series request per resource
	reqCh := make(chan AzureResource, len(resources))
	// resCh (Result Channel) contains the timeseries responses for requests for resource
	resCh := make(chan *Results, len(resources))
	// errCh (Error Channel) contains any errors from requests for
	errCh := make(chan error, len(resources))
	// a worker makes a time series request for a resource
	worker := func() {
		for resource := range reqCh {
			res, err := AzureQuery(prefix, e, T, resource.Type, metric, tagKeysCSV, resource.ResourceGroup, resource.Name, agtype, interval, sdur, edur)
			resCh <- res
			errCh <- err
		}
		defer wg.Done()
	}
	// Create N workers to parallelize multiple requests at once since he resource requires an HTTP request
	for i := 0; i < workerConcurrency; i++ {
		wg.Add(1)
		go worker()
	}
	// Feed resources into the request channel which the workers will consume
	timingString := fmt.Sprintf(`%v queries for metric:"%v" using client "%v"`, len(resources), metric, prefix)
	T.StepCustomTiming("azure", "query-multi", timingString, func() {
		for _, resource := range resources {
			reqCh <- resource
		}
		close(reqCh)
		wg.Wait() // Wait for all the workers to finish
	})
	close(resCh)
	close(errCh)

	// Gather errors from the request and return an error if any of the requests failled
	errors := []string{}
	for err := range errCh {
		if err == nil {
			continue
		}
		errors = append(errors, err.Error())
	}
	if len(errors) > 0 {
		return r, fmt.Errorf(strings.Join(errors, " :: "))
	}
	// Gather all the query results
	for res := range resCh {
		queryResults = append(queryResults, res)
	}
	// Merge the query results into a single seriesSet
	r, err = Merge(e, T, queryResults...)
	return
}

// AzureListResources fetches all resources for the tenant/subscription and caches them for
// up to one minute.
func AzureListResources(prefix string, e *State, T miniprofiler.Timer) (AzureResources, error) {
	// Cache will only last for one minute. In practice this will only apply for web sessions since a
	// new cache is created for each check cycle in the cache
	key := fmt.Sprintf("AzureResourceCache:%s:%s", prefix, time.Now().Truncate(time.Minute*1)) // https://github.com/golang/groupcache/issues/92
	// getFn is a cacheable function for listing azure resources
	getFn := func() (interface{}, error) {
		r := AzureResources{}
		cc, clientFound := e.Backends.AzureMonitor[prefix]
		if !clientFound {
			return r, fmt.Errorf("azure client with name %v not defined", prefix)
		}
		c := cc.ResourcesClient
		// Page through all resources
		for rList, err := c.ListComplete(context.Background(), "", "", nil); rList.NotDone(); err = rList.Next() {
			// TODO not catching auth error here for some reason, err is nil when error!!
			if err != nil {
				return r, err
			}
			val := rList.Value()
			if val.Name != nil && val.Type != nil && val.ID != nil {
				// Extract out the resource group name from the Id
				splitID := strings.Split(*val.ID, "/")
				if len(splitID) < 5 {
					return r, fmt.Errorf("unexpected ID for resource: %s", *val.ID)
				}
				// Add azure tags to the resource
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

// AzureResourcesByType returns all resources of the specified type
// It fetches the complete list resources and then filters them relying on a Cache of that resource list
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

// AzureFilterResources filters a list of resources based on the value of the name, resource group
// or tags associated with that resource
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

// AzureResource is a container for Azure resource information that Bosun can interact with
type AzureResource struct {
	Name          string
	Type          string
	ResourceGroup string
	Tags          map[string]string
}

// AzureResources is a slice of AzureResource
type AzureResources []AzureResource

// Ask makes an AzureResource a github.com/kylebrandt/boolq Asker, which allows it to
// to take boolean expressions to create conditions
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

// AzureMonitorClientCollection is a collection of Azure SDK clients since
// the SDK provides different clients to access different sorts of resources
type AzureMonitorClientCollection struct {
	MetricsClient           insights.MetricsClient
	MetricDefinitionsClient insights.MetricDefinitionsClient
	ResourcesClient         resources.Client
}

// AzureMonitorClients is map of all the AzureMonitorClientCollections that
// have been configured. This is so multiple subscription/tenant/clients
// can be queries from the same Bosun instance using the prefix syntax
type AzureMonitorClients map[string]AzureMonitorClientCollection

// AzureExtractMetricValue is a helper for fetching the value of the requested
// aggregation for the metric
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

// AzureShortAggToLong coverts bosun style names for aggregations (like the reduction functions)
// to the string that is expected for Azure queries
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
	case "count":
		return string(insights.Count), nil
	}
	return "", fmt.Errorf("unrecognized aggregation type %s, must be avg, min, max, or total", agtype)
}
