package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/2017-03-09/resources/mgmt/subscriptions"
	"github.com/Azure/azure-sdk-for-go/profiles/2019-03-01/resources/mgmt/insights"
	"github.com/Azure/azure-sdk-for-go/services/resources/mgmt/2019-05-01/resources"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/to"
)

// AzureResource Parts of azure resource identification
type AzureResource struct {
	Subscription  string
	ResourceGroup string
	Provider      string
	Type          string
	Name          string
	SubType       string
	SubName       string
}

var (
	providers = map[string]string{}
	tenant    = ""
	start     = time.Now().AddDate(0, 0, -89)
)

func main() {
	// create an authorizer from env vars or Azure Managed Service Idenity
	log.Println("Starting app Press CTRL+C to end.")
	authorizer, err := newAuthorizer()
	if err != nil || authorizer == nil {
		log.Fatalf("Impossible to authenticate %#v", err)
	}
	graphAuthorizer, err := newGraphAuthorizer()
	if err != nil || authorizer == nil {
		log.Fatalf("Impossible to authenticate to graph %#v", err)
	}
	var interval = 300
	intervalSrt, intervalConfigured := os.LookupEnv("CHECK_SECONDS_INTERVAL")
	if intervalConfigured {
		interval, err = strconv.Atoi(intervalSrt)
		if err != nil {
			log.Println("CHECK_SECONDS_INTERVAL is not a valid integer")
			interval = 300
		}
	}
	tenantsClient := subscriptions.NewTenantsClient()
	tenantsClient.Authorizer = *authorizer
	tenants, err := tenantsClient.ListComplete(context.Background())
	for tenants.NotDone() {
		value := tenants.Value()
		tenant = *value.TenantID
		tenants.Next()
	}
	subs, err := getSubscriptions(*authorizer)
	providersClient := resources.NewProvidersClient(subs[0])
	providersClient.Authorizer = *authorizer
	providersList, err := providersClient.ListComplete(context.Background(), to.Int32Ptr(50000), "")
	for providersList.NotDone() {
		value := providersList.Value()
		for _, providerType := range *value.ResourceTypes {
			name := fmt.Sprintf("%s/%s", *value.Namespace, *providerType.ResourceType)
			providers[strings.ToLower(name)] = (*providerType.APIVersions)[0]
		}
		providersList.Next()
	}
	executeUpdates(interval, authorizer, graphAuthorizer)
	log.Println("End of schedule")
}

// Method focus of this exercise
func executeUpdates(interval int, authorizer *autorest.Authorizer, graphAuthorizer *autorest.Authorizer) {
	refreshInterval := 5 * time.Second
	quanta := 20                         // How many azure API is allowed to be called per "refreshInterval"
	qc := make(chan interface{}, quanta) // quanta channel, used to throttle azure api call
	ctx := context.Background()

	// TODO: capture the SIGINT/SIGTERM to cancel the `ctx`

	// spawn the refresh quanta go routine, which will cleanup the "qc" every "refreshInterval"
	go refreshQuanta(ctx, qc, refreshInterval)

	timeout := time.Duration(interval * 1e+9)
	ticker := time.NewTicker(timeout)
	for range ticker.C {
		now := time.Now()
		subs, err := getSubscriptions(*authorizer)
		if err != nil {
			log.Panic(err)
		}

		ctx, _ := context.WithTimeout(ctx, timeout)
		var wg sync.WaitGroup
		for _, sub := range subs {
			wg.Add(1)
			go func(ctx context.Context, sub string, start, now time.Time) {
				defer wg.Done()
				evaluateStatus(ctx, *authorizer, *graphAuthorizer, sub, start, now,
					func(p autorest.Preparer) autorest.Preparer {
						return autorest.PreparerFunc(func(r *http.Request) (*http.Request, error) {
							// try to get a quanta before moving on (i.e. sending the azure API)
							qc <- struct{}{}
							return p.Prepare(r)
						})
					})
			}(ctx, sub, start, now)
		}
		wg.Wait()

		back, _ := time.ParseDuration(fmt.Sprintf("-%ds", interval*20))
		start = now.Add(back)
	}
}

func evaluateStatus(
	ctx context.Context,
	auth autorest.Authorizer, authGraph autorest.Authorizer,
	subscription string,
	fromTime time.Time, toTime time.Time, inspector autorest.PrepareDecorator) {
	log.Printf("Evaluating status for: %s", subscription)

	resourceClient := resources.NewClient(subscription)
	activityClient := insights.NewActivityLogsClient(subscription)
	resourceClient.RequestInspector = inspector
	activityClient.RequestInspector = inspector
	activityClient.Authorizer = auth
	resourceClient.Authorizer = auth

	tstarts := fromTime.Format("2006-01-02T15:04:05")
	ts := toTime.Format("2006-01-02T15:04:05")
	filterString := fmt.Sprintf("eventTimestamp ge '%s' and eventTimestamp le '%s'", tstarts, ts)
	listResources, err := activityClient.ListComplete(ctx, filterString, "")
	if err != nil {
		log.Fatal(err)
	}
	for listResources.NotDone() {
		logActivity := listResources.Value()
		listResources.Next()
		if logActivity.Caller == nil || logActivity.ResourceType == nil ||
			logActivity.ResourceType.Value == nil ||
			*logActivity.ResourceType.Value == "Microsoft.Resources/deployments" ||
			unsuportedProviders[strings.ToLower(*logActivity.ResourceType.Value)] ||
			logActivity.SubStatus == nil || logActivity.SubStatus.Value == nil ||
			(*logActivity.SubStatus.Value != "Created" && !writeOperation.MatchString(*logActivity.OperationName.Value)) {
			continue
		}
		resourceID := *logActivity.ResourceID
		apiVersion := providers[strings.ToLower(*logActivity.ResourceType.Value)]
		if apiVersion == "" {
			log.Println(strings.ToLower(*logActivity.ResourceType.Value))
			continue
		}
		res, err := resourceClient.GetByID(ctx, resourceID, apiVersion)

		if res.Response.StatusCode != 404 && err != nil {
			log.Println("REAL ERROR", err)
			continue
		} else if res.Response.StatusCode == 404 {
			continue
		}

		resID := getResource(*res.ID)

		if res.Tags["Created-by"] == nil {
			if res.Tags == nil {
				res.Tags = map[string]*string{}
			}
			name := "UNKNOWN"
			if logActivity.Claims["name"] != nil {
				name = fmt.Sprintf("%s", *logActivity.Caller)
			} else if logActivity.Claims["appid"] != nil {
				appName, _ := getAppName(logActivity.Claims["appid"], authGraph)
				name = fmt.Sprintf("%s", appName)
			}
			log.Printf("UPDATING %s | %s | %s | %s", resID.Subscription, resID.Name, strings.ToLower(*logActivity.ResourceType.Value), name)
			res.Tags["Created-by"] = to.StringPtr(name)
			res.Tags["Created-by-id"] = logActivity.Caller
			resUpdate := resources.GenericResource{
				ID:   res.ID,
				Tags: res.Tags,
			}
			_, err := resourceClient.UpdateByID(ctx, *resUpdate.ID, apiVersion, resUpdate)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func refreshQuanta(ctx context.Context, c chan interface{}, refreshInterval time.Duration) {
	go func() {
		tick := time.NewTicker(refreshInterval)
		for range tick.C {
			// Check whether need to quit
			select {
			case <-ctx.Done():
				return
			default:
				break
			}
			// Refresh quanta via freeing the throttle channel.
			// Note: In order to avoid the case that there are many backlog jobs
			//       trying to fetch the quanta, we will have to fix the amount of
			//       quanta to release.
		ExhaustLoop:
			for i := len(c); i > 0; i-- {
				select {
				case <-c:
					continue
				default:
					break ExhaustLoop
				}
			}
		}
	}()
}
