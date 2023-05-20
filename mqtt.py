import asyncio
import os
import json
from amqtt.client import MQTTClient
from amqtt.mqtt.constants import QOS_0
from data_models.measurement import ACMeasurement, DCMeasurement

from typing import Tuple, List

from data_provider.influx_repository import InfluxRepository


class MQTTMeasurementHandler:
    def __init__(self, mqtt_broker_url, mqtt_username, mqtt_password, influx_repo, topics):
        self.mqtt_broker_url = mqtt_broker_url
        self.mqtt_username = mqtt_username
        self.mqtt_password = mqtt_password
        self.influx_repo = influx_repo
        self.client = MQTTClient()
        self.topics: List[Tuple[str, int]] = topics

    async def parse_payload(self, payload: str) -> [ACMeasurement, DCMeasurement]:
        pass

    async def handle_measurement(self, topic, payload):
        # Parse the payload into a measurement object
        measurement = self.parse_payload(payload)

        # Determine the measurement type based on the topic
        if topic == "ac_measurement_topic":
            self.influx_repo.write_ac_measurement(measurement)
        elif topic == "dc_measurement_topic":
            self.influx_repo.write_dc_measurement(measurement)

    async def mqtt_message_received(self, topic, payload, qos, retain):
        # Handle the measurement asynchronously
        asyncio.create_task(self.handle_measurement(topic, payload))

    async def connect(self):
        # Connect to the MQTT broker
        await self.client.connect(self.mqtt_broker_url)

        # Subscribe to the measurement topics
        await self.client.subscribe([
            ("ac_measurement_topic", 0),
            ("dc_measurement_topic", 0)
        ])

        # Start listening for incoming messages
        self.client.message_callback_add("#", self.mqtt_message_received)
        await self.client.loop_start()

    async def disconnect(self):
        # Clean up and disconnect from the MQTT broker
        await self.client.disconnect()


async def main():
    # Instantiate the InfluxRepository
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "siegel"
    url = "http://localhost:8086"
    influx_repo = InfluxRepository(org, token, url)

    # Instantiate the MQTTMeasurementHandler
    mqtt_handler = MQTTMeasurementHandler(
        mqtt_broker_url="broker.hivemq.com",
        influx_repo=influx_repo
    )

    # Connect to the MQTT broker and handle measurements
    await mqtt_handler.connect()

    # Keep the program running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass

    # Disconnect from the MQTT broker
    await mqtt_handler.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
