package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/configurate"
	"github.com/spf13/viper"

	"github.com/cyverse-de/messaging"
	//"github.com/streadway/amqp"
)

const defaultConfig = `
amqp:
  uri: amqp://guest:guest@rabbit:5672/
  queue_prefix: ""

infosquito:
  maximum_in_prefix: 10000

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
)

func init() {
	flag.Parse()
	logrus.SetFormatter(&logrus.JSONFormatter{})
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
		err := ReindexPrefix(db, es, "0000")
		if err != nil {
			log.Fatal(err)
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

	spin()
}
