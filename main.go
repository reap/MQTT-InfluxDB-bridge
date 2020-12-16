package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb1-client/v2"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	log "github.com/sirupsen/logrus"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerURL string
var dbURL string

func sendValueToDatabase(database string, source string, sensor string, value float64) {
	msgLog := log.WithFields(log.Fields{
		"database": database,
		"source":   source,
		"sensor":   sensor,
	})
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: dbURL,
	})
	if err != nil {
		msgLog.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})
	if err != nil {
		msgLog.Fatal(err)
	}

	// Create a point and add to batch
	tags := map[string]string{"source": source}

	fields := map[string]interface{}{
		"value": value,
	}

	pt, err := client.NewPoint(sensor, tags, fields, time.Now())
	if err != nil {
		msgLog.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		msgLog.Fatal(err)
	}

}

func receiveMQTTMessage(ctx context.Context, receiveChannel <-chan MQTT.Message) {
	for {
		select {
		case <-ctx.Done():
			log.Info("Context cancelled, quitting listener...")
			return
		case message := <-receiveChannel:
			msgLog := log.WithFields(log.Fields{
				"topic":   message.Topic(),
				"payload": string(message.Payload()),
			})

			msgLog.Info("Received message")

			topicParts := strings.Split(message.Topic(), "/")

			if len(topicParts) >= 3 {
				database := topicParts[1]
				source := topicParts[2]
				sensor := strings.Join(topicParts[3:len(topicParts)], "-")

				payload, err := strconv.ParseFloat(string(message.Payload()), 32)

				if err != nil {
					msgLog.WithFields(log.Fields{
						"error": err,
					}).Info("Failed to parse number for payload")
					continue
				}
				sendValueToDatabase(database, source, sensor, payload)
			} else {
				msgLog.Info("Message did not contain expected topic parts, skipping it")
			}
		}

	}
}

func createMQTTClient(brokerURL string, channel chan<- MQTT.Message) {
	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("go-server")

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		channel <- msg
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
}

func main() {
	readConfiguration()

	receiveChannel := make(chan MQTT.Message)

	createMQTTClient(brokerURL, receiveChannel)

	var wg sync.WaitGroup

	// Handle interrupt and term signals gracefully
	ctx, cancel := context.WithCancel(context.Background())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		wg.Add(1)
		select {
		case <-sigs:
			cancel()
		case <-ctx.Done():
		}
		wg.Done()
	}()

	wg.Add(1)
	// start listening to input channel
	go func(ctx context.Context, channel chan MQTT.Message) {
		log.Info("Starting listener")
		defer wg.Done()
		receiveMQTTMessage(ctx, channel)
		log.Info("Listener returned")
	}(ctx, receiveChannel)

	log.Infof("Listener running")
	wg.Wait()
}

func readConfiguration() {
	// allow setting values also with commandline flags
	pflag.String("broker", "tcp://localhost:1883", "MQTT-broker url")
	viper.BindPFlag("broker", pflag.Lookup("broker"))

	pflag.String("db", "http://localhost:8086", "Influx database connection url")
	viper.BindPFlag("db", pflag.Lookup("db"))

	// parse values from environment variables
	viper.AutomaticEnv()

	brokerURL = viper.GetString("broker")
	dbURL = viper.GetString("db")

	log.Infof("Using broker %s", brokerURL)
	log.Infof("Using database %s", dbURL)
}
