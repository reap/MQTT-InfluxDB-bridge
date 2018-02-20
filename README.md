# MQTT-InfluxDB-bridge

Application listens for MQTT-broker and stores messages to InfluxDB.

    value = 25
    topic = home/room1/temperature

is sent to InfluxDB with values

    database   = home
    serie      = temperature
    value      = 25
    tag:source = room1

## Running

To run program needs two arguments 
* ```broker``` - connection string to MQTT-broker
* ```db``` - connection string to InfluxDB

Arguments can be provided as command line flags, for example:

    ./mqtt-influx-bridge -broker=tcp:/localhost:1883 -db=http://localhost:8086

or environment variables:

    BROKER=tcp:/localhost:1883 DB=http://localhost:8086 ./mqtt-influx-bridge