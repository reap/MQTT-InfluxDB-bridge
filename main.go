package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/influxdata/influxdb/client/v2"

	log "github.com/sirupsen/logrus"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerURL string
var dbURL string

func receiveMQTTMessage(ctx context.Context, receiveChannel chan MQTT.Message) {

	matcher, _ := regexp.Compile("/mini-iot/([a-z-]+)/([a-z-]+)")

	/*
	  The incoming temperature/humidity messages are from topic like
	    "/home/[room]/[sensor]" for example "/home/computer-room/temperature"
	*/
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

			if !matcher.MatchString(message.Topic()) {
				msgLog.Info("Unknown topic")
				continue
			}

			msgLog.Info("Matched topic, continue to get the topic-values")
			matches := matcher.FindStringSubmatch(message.Topic())
			msgLog.Info(matches)

			room := matches[1]
			sensor := matches[2]

			payload, err := strconv.ParseFloat(string(message.Payload()), 32)

			if err != nil {
				msgLog.WithFields(log.Fields{
					"error": err,
				}).Info("Failed to parse number for payload")
				continue
			}

			c, err := client.NewHTTPClient(client.HTTPConfig{
				Addr: dbURL,
			})
			if err != nil {
				log.Fatal(err)
			}

			// Create a new point batch
			bp, err := client.NewBatchPoints(client.BatchPointsConfig{
				Database:  "Temperatures",
				Precision: "s",
			})
			if err != nil {
				log.Fatal(err)
			}

			// Create a point and add to batch
			tags := map[string]string{"room": room, "sensor": sensor}

			fields := map[string]interface{}{
				"value": payload,
			}

			pt, err := client.NewPoint("temperature", tags, fields, time.Now())
			if err != nil {
				log.Fatal(err)
			}
			bp.AddPoint(pt)

			// Write the batch
			if err := c.Write(bp); err != nil {
				msgLog.Fatal(err)
			}
		default:
			// log.Println("No messages")
		}

	}
}

func main() {
	flag.StringVar(&brokerURL, "broker", "tcp://localhost:1883", "MQTT-broker url")
	flag.StringVar(&dbURL, "db", "http://localhost:8086", "Influx database connection url")

	flag.Parse()

	log.Infof("Using broker %s", brokerURL)
	log.Infof("Using database %s", dbURL)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("go-server")

	receiveChannel := make(chan MQTT.Message)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		receiveChannel <- msg
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

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
