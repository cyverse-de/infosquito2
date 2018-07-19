package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/configurate"
	"github.com/spf13/viper"

	"github.com/cyverse-de/messaging"
	"github.com/streadway/amqp"
)

const defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/
  queue_prefix: ""

infosquito:
  maximum_in_prefix: 10000
  base_prefix_length: 3

elasticsearch:
  base: http://elasticsearch:9200
  index: data

db:
  uri: postgres://ICAT:fakepassword@icat-db:5432/ICAT?sslmode=disable
`

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

	listenClient  *messaging.Client
	publishClient *messaging.Client

	amqpURI               string
	amqpExchangeName      string
	amqpExchangeType      string
	amqpQueuePrefix       string
	elasticsearchBase     string
	elasticsearchUser     string
	elasticsearchPassword string
	elasticsearchIndex    string
	dbURI                 string
	maxInPrefix           int
	basePrefixLength      int
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
	for {
		select {
		case <-spinner:
			fmt.Println("Exiting")
			break
		}
	}
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
		log.Fatal(err)
	}

	dbURI = cfg.GetString("db.uri")
	elasticsearchBase = cfg.GetString("elasticsearch.base")
	elasticsearchUser = cfg.GetString("elasticsearch.user")
	elasticsearchPassword = cfg.GetString("elasticsearch.password")
	elasticsearchIndex = cfg.GetString("elasticsearch.index")
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

func TryReindexPrefix(db *ICATConnection, es *ESConnection, prefix string) error {
	err := ReindexPrefix(db, es, prefix)
	if err == ErrTooManyResults {
		for _, newprefix := range splitPrefix(prefix) {
			err = TryReindexPrefix(db, es, newprefix)
			if err != nil {
				return err
			}
		}
	} else if err != nil {
		return err
	}
	return nil
}

func main() {
	checkMode()
	initConfig(*cfgPath)

	db, err := SetupDB(dbURI)
	if err != nil {
		log.Fatal(err)
	}

	es, err := SetupES(elasticsearchBase, elasticsearchUser, elasticsearchPassword, elasticsearchIndex)
	if err != nil {
		log.Fatal(err)
	}

	if *mode == "full" {
		log.Info("Full indexing mode selected.")
		// do full mode
		for _, prefix := range generatePrefixes(basePrefixLength) {
			log.Infof("Reindexing prefix %s", prefix)
			err := TryReindexPrefix(db, es, prefix)
			if err != nil {
				log.Fatal(err)
			}
		}
		return
	}

	// periodic mode
	log.Info("Periodic indexing mode selected.")
	loadAMQPConfig()

	listenClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatal(err)
	}
	defer listenClient.Close()

	publishClient, err := messaging.NewClient(amqpURI, true)
	if err != nil {
		log.Fatal(err)
	}
	defer publishClient.Close()

	publishClient.SetupPublishing(amqpExchangeName)

	go listenClient.Listen()

	queueName := getQueueName(amqpQueuePrefix)
	listenClient.AddConsumerMulti(
		amqpExchangeName,
		amqpExchangeType,
		queueName,
		[]string{"index.all", "index.data", "index.data.prefix.#"},
		func(del amqp.Delivery) {
			var err error
			log.Infof("Got message %s", del.RoutingKey)
			if del.RoutingKey == "index.all" || del.RoutingKey == "index.data" {
				for _, prefix := range generatePrefixes(basePrefixLength) {
					err = publishClient.Publish(fmt.Sprintf("index.data.prefix.%s", prefix), []byte{})
					if err != nil {
						del.Reject(!del.Redelivered)
						return
					}
				}
			} else {
				prefix := del.RoutingKey[18:]
				log.Infof("Triggered reindexing prefix %s", prefix)
				err = ReindexPrefix(db, es, prefix)
				if err == ErrTooManyResults {
					log.Infof("Prefix %s too large, splitting", prefix)
					for _, newprefix := range splitPrefix(prefix) {
						err = publishClient.Publish(fmt.Sprintf("index.data.prefix.%s", newprefix), []byte{})
						if err != nil {
							del.Reject(!del.Redelivered)
							return
						}
					}
				} else if err != nil {
					del.Reject(!del.Redelivered)
					return
				}
			}
			del.Ack(false)
			return
		},
		1)

	spin()
}
