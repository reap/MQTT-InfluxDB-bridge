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

    ./mqtt-influx-bridge -broker=tcp:/localhost:1883 -db=http://localhost:8086