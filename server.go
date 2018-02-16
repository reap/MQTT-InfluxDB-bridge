package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/client/v2"

	MQTT "github.com/eclipse/paho.mqtt.golang"
)

var brokerURL string
var dbURL string

func receiveMQTTMessage(receiveChannel chan MQTT.Message) {

	matcher, _ := regexp.Compile("/mini-iot/([a-z-]+)/([a-z-]+)")

	log.Println("goroutine: Starting to listen for requests")
	/*
	  The incoming temperature/humidity messages are from topic like
	    "/home/[room]/[sensor]" for example "/home/computer-room/temperature"
	*/
	for {
		message := <-receiveChannel
		log.Printf("Received topic: %s, message: %s\n", message.Topic(), string(message.Payload()))

		if matcher.MatchString(message.Topic()) {
			log.Print("Matched topic, continue to get the topic-values")
			matches := matcher.FindStringSubmatch(message.Topic())
			log.Print(matches)

			room := matches[1]
			sensor := matches[2]

			payload, err := strconv.ParseFloat(string(message.Payload()), 32)

			if err != nil {
				log.Print("Failed to parse number for payload")
				log.Print(err)
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
				log.Fatal(err)
			}

		} else {
			log.Printf("Unkown topic: %s in message: %s, ignoring it...\n", message.Topic(), string(message.Payload()))
		}

	}
}

func main() {
	flag.StringVar(&brokerURL, "broker", "tcp://localhost", "MQTT-broker url")
	flag.StringVar(&dbURL, "db", "http://localhost:8086", "Influx database connection url")

	flag.Parse()

	fmt.Println(brokerURL)
	fmt.Println(dbURL)

	opts := MQTT.NewClientOptions()
	opts.AddBroker(brokerURL)
	opts.SetClientID("go-server")

	reveiceChannel := make(chan MQTT.Message)

	opts.SetDefaultPublishHandler(func(client MQTT.Client, msg MQTT.Message) {
		reveiceChannel <- msg
	})

	client := MQTT.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := client.Subscribe("#", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}
	log.Println("starting go-routine")
	go receiveMQTTMessage(reveiceChannel)

	for {
	}
}
