package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/client/v2"

	log "github.com/sirupsen/logrus"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerURL string
var dbURL string

func sendValueToDatabase(database string, source string, sensor string, value float64) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: dbURL,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  database,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create a point and add to batch
	tags := map[string]string{"source": source}

	fields := map[string]interface{}{
		"value": value,
	}

	pt, err := client.NewPoint(sensor, tags, fields, time.Now())
	if err != nil {
		log.Fatal(err)
	}
	bp.AddPoint(pt)

	// Write the batch
	if err := c.Write(bp); err != nil {
		log.Fatal(err)
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

			database := topicParts[0]
			source := topicParts[1]
			sensor := strings.Join(topicParts[2:len(topicParts)], "-")

			payload, err := strconv.ParseFloat(string(message.Payload()), 32)

			if err != nil {
				msgLog.WithFields(log.Fields{
					"error": err,
				}).Info("Failed to parse number for payload")
				continue
			}
			sendValueToDatabase(database, source, sensor, payload)
		default:
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
	flag.StringVar(&brokerURL, "broker", "tcp://localhost:1883", "MQTT-broker url")
	flag.StringVar(&dbURL, "db", "http://localhost:8086", "Influx database connection url")

	flag.Parse()

	log.Infof("Using broker %s", brokerURL)
	log.Infof("Using database %s", dbURL)

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
