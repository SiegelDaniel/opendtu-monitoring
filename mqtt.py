import asyncio
import logging
import os
import struct

from amqtt.client import MQTTClient
from data_models.measurement import MetaInfo
from logging import getLogger

from typing import Tuple, List

from data_provider.influx_repository import InfluxRepository


class MQTTMeasurementHandler:
    def __init__(self, mqtt_broker_url, influx_repo, topics):
        self.mqtt_broker_url = mqtt_broker_url
        self.influx_repo = influx_repo
        self.client = MQTTClient()
        self.topics: List[Tuple[str, int]] = topics
        self.logger = getLogger("MeasurementHandlerLogger")
        self.logger.addHandler(logging.StreamHandler())

    async def handle_measurement(self, topic: str, data: bytearray) -> []:
        self.logger.debug("Handling measurement")
        datastr = topic.split("/")

        topic = datastr[3]
        channel = datastr[2]
        serial = datastr[1]

        meta_info = MetaInfo(channel=channel, serial=serial)
        measurement = self.influx_repo.topics_to_measurements[topic]
        data = float(data.decode('utf-8'))
        easurement = measurement(meta_info, data)

        self.influx_repo.insert_method_by_topic[topic](measurement)


    async def connect(self):
        self.logger.debug("Connected to broker at {0}".format(self.mqtt_broker_url))
        # Connect to the MQTT broker
        await self.client.connect(self.mqtt_broker_url)

        # Subscribe to the measurement topics
        await self.client.subscribe(self.topics)

    async def disconnect(self):
        self.logger.debug("Disconnected from broker.")
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
            await mqtt_handler.handle_measurement(message.topic, message.data)
    except KeyboardInterrupt:
        pass

    # Disconnect from the MQTT broker
    await mqtt_handler.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
