import asyncio
import os
from amqtt.client import MQTTClient
from data_models.measurement import ACMeasurement, DCMeasurement

from typing import Tuple, List

from data_provider.influx_repository import InfluxRepository


class MQTTMeasurementHandler:
    def __init__(self, mqtt_broker_url, influx_repo, topics):
        self.mqtt_broker_url = mqtt_broker_url
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

    async def connect(self):
        # Connect to the MQTT broker
        await self.client.connect(self.mqtt_broker_url)

        # Subscribe to the measurement topics
        await self.client.subscribe(self.topics)

    async def disconnect(self):
        # Clean up and disconnect from the MQTT broker
        await self.client.disconnect()


async def main():
    # Instantiate the InfluxRepository
    token = os.environ.get("INFLUXDB_TOKEN")
    org = "siegel"
    url = "http://localhost:8086"
    topics = [
        ("/114181804132/0/voltage", 0),
        ("/114181804132/0/current", 0),
        ("/114181804132/1/voltage", 0),
        ("/114181804132/2/voltage", 0),
        ("/114181804132/0/yieldday", 0),
        ("/114181804132/0/yieldtotal", 0)
    ]
    influx_repo = InfluxRepository(org, token, url)

    # Instantiate the MQTTMeasurementHandler
    mqtt_handler = MQTTMeasurementHandler(
        mqtt_broker_url="mqtt://test.mosquitto.org:1883",
        influx_repo=influx_repo,
        topics=topics
    )

    await mqtt_handler.connect()

    # Keep the program running until interrupted
    try:
        while True:
            message = await mqtt_handler.client.deliver_message()
            print(str(message.data) + "\t" + str(message.topic))
    except KeyboardInterrupt:
        pass

    # Disconnect from the MQTT broker
    await mqtt_handler.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
