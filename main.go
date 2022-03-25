package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/configurate"
	"github.com/spf13/viper"

	"github.com/cyverse-de/messaging/v9"
	"github.com/streadway/amqp"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

const otelName = "github.com/cyverse-de/infosquito2"
const defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/
  queue_prefix: ""
  dewey_uri: amqp://guest:guest@rabbit:5672/
  dewey_queue: "dewey.indexing"

irods:
  zone: iplant

infosquito:
  maximum_in_prefix: 10000
  base_prefix_length: 3

elasticsearch:
  base: http://elasticsearch:9200
  index: data

db:
  uri: postgres://ICAT:fakepassword@icat-db:5432/ICAT?sslmode=disable
`

const prefixRoutingKey string = "index.data.prefix"
const prefixRoutingKeyLen int = len(prefixRoutingKey)

var log = logrus.WithFields(logrus.Fields{
	"service": "infosquito2",
	"art-id":  "infosquito2",
	"group":   "org.cyverse",
})

var (
	cfgPath = flag.String("config", "", "Path to the configuration file.")
	mode    = flag.String("mode", "", "One of 'periodic' or 'full'.")
	debug   = flag.Bool("debug", false, "Set to true to enable debug logging")
	cfg     *viper.Viper

	amqpURI               string
	amqpDeweyURI          string
	amqpExchangeName      string
	amqpExchangeType      string
	amqpQueuePrefix       string
	amqpDeweyQueue        string
	elasticsearchBase     string
	elasticsearchUser     string
	elasticsearchPassword string
	elasticsearchIndex    string
	irodsZone             string
	dbURI                 string
	maxInPrefix           int
	basePrefixLength      int

	tracerProvider *tracesdk.TracerProvider
)

func init() {
	flag.Parse()
	logrus.SetFormatter(&logrus.JSONFormatter{})
	if !(*debug) {
		logrus.SetLevel(logrus.InfoLevel)
	} else {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

func spin() {
	spinner := make(chan int)
	<-spinner
}

func checkMode() {
	if *mode != "periodic" && *mode != "full" {
		fmt.Printf("Invalid mode: %s\n", *mode)
		flag.PrintDefaults()
		os.Exit(-1)
	}
}

func initConfig(cfgPath string) {
	var err error
	cfg, err = configurate.InitDefaults(cfgPath, defaultConfig)
	if err != nil {
		log.Fatalf("Unable to initialize the default configuration settings: %s", err)
	}

	dbURI = cfg.GetString("db.uri")
	elasticsearchBase = cfg.GetString("elasticsearch.base")
	elasticsearchUser = cfg.GetString("elasticsearch.user")
	elasticsearchPassword = cfg.GetString("elasticsearch.password")
	elasticsearchIndex = cfg.GetString("elasticsearch.index")
	irodsZone = cfg.GetString("irods.zone")
	max, err := strconv.Atoi(cfg.GetString("infosquito.maximum_in_prefix"))
	if err != nil {
		log.Fatal("Couldn't parse integer out of infosquito.maximum_in_prefix")
	}
	maxInPrefix = max

	base, err := strconv.Atoi(cfg.GetString("infosquito.base_prefix_length"))
	if err != nil {
		log.Fatal("Couldn't parse integer out of infosquito.base_prefix_length")
	}
	basePrefixLength = base
}

func loadAMQPConfig() {
	amqpURI = cfg.GetString("amqp.uri")
	amqpExchangeName = cfg.GetString("amqp.exchange.name")
	amqpExchangeType = cfg.GetString("amqp.exchange.type")
	amqpQueuePrefix = cfg.GetString("amqp.queue_prefix")

	amqpDeweyURI = cfg.GetString("amqp.dewey_uri")
	amqpDeweyQueue = cfg.GetString("amqp.dewey_queue")
}

func getQueueName(prefix string) string {
	if len(prefix) > 0 {
		return fmt.Sprintf("%s.infosquito2", prefix)
	}
	return "infosquito2"
}

func generatePrefixes(length int) []string {
	prefixes := int(math.Pow(16, float64(length)))
	res := make([]string, prefixes)
	for i := 0; i < prefixes; i++ {
		res[i] = fmt.Sprintf("%0"+strconv.Itoa(length)+"x", i)
	}
	return res
}

func splitPrefix(prefix string) []string {
	res := make([]string, 16)
	for i := 0; i < 16; i++ {
		res[i] = fmt.Sprintf("%s%x", prefix, i)
	}
	return res
}

func tryReindexPrefix(context context.Context, db *ICATConnection, es *ESConnection, prefix, irodsZone string) error {
	err := ReindexPrefix(context, db, es, prefix, irodsZone)
	if err == ErrTooManyResults {
		for _, newprefix := range splitPrefix(prefix) {
			err = tryReindexPrefix(context, db, es, newprefix, irodsZone)
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}
	return nil
}

func publishPrefixMessages(context context.Context, prefixes []string, client *messaging.Client, del amqp.Delivery) error {
	log.Infof("Publishing %d prefix messages", len(prefixes))
	for _, prefix := range prefixes {
		err := client.PublishContext(context, fmt.Sprintf("%s.%s", prefixRoutingKey, prefix), []byte{})
		if err != nil {
			rejectErr := del.Reject(!del.Redelivered)
			if rejectErr != nil {
				log.Error(rejectErr)
			}
			return err
		}
	}
	return nil
}

func handleIndex(context context.Context, del amqp.Delivery, publishClient *messaging.Client, deweyClient *messaging.Client) error {
	ctx, span := otel.Tracer(otelName).Start(context, "handleIndex")
	defer span.End()

	log.Infof("Purging dewey queue %s", amqpDeweyQueue)
	err := deweyClient.PurgeQueue(amqpDeweyQueue)
	if err != nil {
		log.Error(err)
	}
	return publishPrefixMessages(ctx, generatePrefixes(basePrefixLength), publishClient, del)
}

func handlePrefix(context context.Context, del amqp.Delivery, db *ICATConnection, es *ESConnection, publishClient *messaging.Client) error {
	ctx, span := otel.Tracer(otelName).Start(context, "handlePrefix")
	defer span.End()

	prefix := del.RoutingKey[prefixRoutingKeyLen+1:]
	log.Debugf("Triggered reindexing prefix %s", prefix)
	err := ReindexPrefix(ctx, db, es, prefix, irodsZone)
	if err == ErrTooManyResults {
		log.Infof("Prefix %s too large, splitting", prefix)
		return publishPrefixMessages(ctx, splitPrefix(prefix), publishClient, del)
	} else if err != nil {
		log.Errorf("Error reindexing prefix %s: %s", prefix, err)
		rejectErr := del.Reject(!del.Redelivered)
		if rejectErr != nil {
			log.Error(rejectErr)
		}
		return err
	}

	return nil
}

func jaegerTracerProvider(url string) (*tracesdk.TracerProvider, error) {
	// Create the Jaeger exporter
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("infosquito2"),
		)),
	)

	return tp, nil
}

func main() {

	checkMode()
	initConfig(*cfgPath)

	otelTracesExporter := os.Getenv("OTEL_TRACES_EXPORTER")
	if otelTracesExporter == "jaeger" {
		jaegerEndpoint := os.Getenv("OTEL_EXPORTER_JAEGER_ENDPOINT")
		if jaegerEndpoint == "" {
			log.Warn("Jaeger set as OpenTelemetry trace exporter, but no Jaeger endpoint configured.")
		} else {
			tp, err := jaegerTracerProvider(jaegerEndpoint)
			if err != nil {
				log.Fatal(err)
			}
			tracerProvider = tp
			otel.SetTracerProvider(tp)
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
		}
	}

	if tracerProvider != nil {
		tracerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		defer func(tracerContext context.Context) {
			ctx, cancel := context.WithTimeout(tracerContext, time.Second*5)
			defer cancel()
			if err := tracerProvider.Shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}(tracerCtx)
	}

	db, err := SetupDB(dbURI)
	if err != nil {
		log.Fatalf("Unable to set up the database: %s", err)
	}

	es, err := SetupES(elasticsearchBase, elasticsearchUser, elasticsearchPassword, elasticsearchIndex)
	if err != nil {
		log.Fatalf("Unable to set up the ElasticSearch connection: %s", err)
	}

	if *mode == "full" {
		log.Info("Full indexing mode selected.")
		// do full mode
		for _, prefix := range generatePrefixes(basePrefixLength) {
			log.Infof("Reindexing prefix %s", prefix)
			err = tryReindexPrefix(context.Background(), db, es, prefix, irodsZone)
			if err != nil {
				log.Fatalf("Full reindexing failed: %s", err)
			}
		}
		return
	}

	// periodic mode
	log.Info("Periodic indexing mode selected.")
	loadAMQPConfig()

	listenClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging listen client: %s", err)
	}
	defer listenClient.Close()

	publishClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging publish client: %s", err)
	}
	defer publishClient.Close()

	err = publishClient.SetupPublishing(amqpExchangeName)
	if err != nil {
		log.Fatalf("Unable to set up message publishing: %s", err)
	}

	deweyClient, err := messaging.NewClient(amqpDeweyURI, true)
	if err != nil {
		log.Fatalf("Unable to create the messaging dewey client: %s", err)
	}
	defer deweyClient.Close()

	go listenClient.Listen()

	queueName := getQueueName(amqpQueuePrefix)
	listenClient.AddConsumerMulti(
		amqpExchangeName,
		amqpExchangeType,
		queueName,
		[]string{"index.all", "index.data", fmt.Sprintf("%s.#", prefixRoutingKey)},
		func(context context.Context, del amqp.Delivery) {
			var err error
			log.Debugf("Got message %s", del.RoutingKey)
			if del.RoutingKey == "index.all" || del.RoutingKey == "index.data" {
				err = handleIndex(context, del, publishClient, deweyClient)
			} else {
				err = handlePrefix(context, del, db, es, publishClient)
			}
			if err != nil {
				return
			}
			err = del.Ack(false)
			if err != nil {
				log.Error(err)
			}
		},
		1)

	spin()
}
