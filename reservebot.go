package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/ameliagapin/reservebot/data"
	"github.com/ameliagapin/reservebot/handler"
	"github.com/ameliagapin/reservebot/util"
	log "github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
)

var (
	token          string
	challenge      string
	appToken       string
	listenPort     int
	debug          bool
	admins         string
	reqResourceEnv bool
	pruneEnabled   bool
	pruneInterval  int
	pruneExpire    int
	redisAddr      string
	redisPass      string
	redisDB        int
	useRedis       bool
)

func main() {
	flag.StringVar(&token, "token", util.LookupEnvOrString("SLACK_TOKEN", ""), "Slack API Token")
	flag.StringVar(&challenge, "challenge", util.LookupEnvOrString("SLACK_CHALLENGE", ""), "Slack verification token")
	flag.StringVar(&appToken, "appToken", util.LookupEnvOrString("SLACK_APP_TOKEN", ""), "Slack App token")

	flag.IntVar(&listenPort, "listen-port", util.LookupEnvOrInt("LISTEN_PORT", 666), "Listen port")

	flag.BoolVar(&debug, "debug", util.LookupEnvOrBool("DEBUG", false), "Debug mode")

	flag.StringVar(&admins, "admins", util.LookupEnvOrString("SLACK_ADMINS", ""), "Turn on administrative commands for specific admins, comma separated list")

	flag.BoolVar(&reqResourceEnv, "require-resource-env", util.LookupEnvOrBool("REQUIRE_RESOURCE_ENV", true), "Require resource reservation to include environment")

	flag.BoolVar(&pruneEnabled, "prune-enabled", util.LookupEnvOrBool("PRUNE_ENABLED", true), "Enable pruning available resources automatically")
	flag.IntVar(&pruneInterval, "prune-interval", util.LookupEnvOrInt("PRUNE_INTERVAL", 1), "Automatic pruning interval in hours")
	flag.IntVar(&pruneExpire, "prune-expire", util.LookupEnvOrInt("PRUNE_EXPIRE", 168), "Automatic prune expiration time in hours")

	flag.StringVar(&redisAddr, "redis-address", util.LookupEnvOrString("REDIS_ADDRESS", "localhost:6379"), "Redis Database Address")
	flag.StringVar(&redisPass, "redis-pw", util.LookupEnvOrString("REDIS_PASS", ""), "Redis Database Password")
	flag.IntVar(&redisDB, "redis-database", util.LookupEnvOrInt("REDIS_DB", 0), "Redis Database")
	flag.BoolVar(&useRedis, "use-redis", util.LookupEnvOrBool("USE_REDIS", false), "Activate redis db")

	flag.Parse()

	// Make sure required vars are set
	if token == "" {
		log.Error("Slack token is required")
		return
	}
	if challenge == "" {
		log.Error("Slack verification token is required")
		return
	}
	log.Info(token, appToken)
	api := slack.New(
		token,
		slack.OptionDebug(debug),
		slack.OptionAppLevelToken(appToken),
	)
	var d data.Manager
	d = data.NewMemory()
	if useRedis {
		log.Infof("Redis Enabled")
		log.Infof(redisPass)
		d = data.NewRedis(redisAddr, redisPass, redisDB)
	}
	if pruneEnabled {
		// Prune inactive resources
		log.Infof("Automatic Pruning is enabled.")
		go func() {
			for {
				time.Sleep(time.Duration(pruneInterval) * time.Hour)
				err := d.PruneInactiveResources(pruneExpire)
				if err != nil {
					log.Errorf("Error pruning resources: %+v", err)
				} else {
					log.Infof("Pruned resources")
				}
			}
		}()
	} else {
		log.Infof("Automatic pruning is disabled.")
	}

	handler := handler.New(api, d, reqResourceEnv, util.ParseAdmins(admins))

	client := socketmode.New(
		api,
		socketmode.OptionDebug(true),
	)

	go func() {
		for evt := range client.Events {
			switch evt.Type {
			case socketmode.EventTypeConnecting:
				log.Info("Connecting to Slack with Socket Mode...")
			case socketmode.EventTypeConnectionError:
				log.Info("Connection failed. Retrying later...")
			case socketmode.EventTypeConnected:
				log.Info("Connected to Slack with Socket Mode.")
			case socketmode.EventTypeEventsAPI:
				eventsAPIEvent, ok := evt.Data.(slackevents.EventsAPIEvent)
				if !ok {
					fmt.Printf("Ignored %+v\n", evt)
					continue
				}

				fmt.Printf("Event received: %+v\n", eventsAPIEvent)
				client.Ack(*evt.Request)

				if err := handler.CallbackEvent(eventsAPIEvent); err != nil {
					log.Errorf("%+v", err)
				}
			default:
				fmt.Fprintf(os.Stderr, "Unexpected event type received: %s\n", evt.Type)
			}
		}
	}()
	log.Infof("Starting Event Socket %d", listenPort)
	client.Run()

}
