import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from data_models.measurement import MetaInfo

token = os.environ.get("INFLUXDB_TOKEN")
org = "siegel"
url = "http://localhost:8086"


class InfluxRepository:
    def __init__(self, org: str, token: str, url: str = "http://localhost:8086") -> None:
        self.write_client = InfluxDBClient(url=url, token=token, org=org)
        self.message_buffer = []

        self.insert_method_by_topic = {
            "voltage": self.write_voltage,
            "current": self.write_current,
            "yieldday": self.write_yield_today,
            "temperature": self.write_temperature
        }

    def write_voltage(self, meta: MetaInfo, voltage: float):
        point = Point("voltage_measurement") \
            .tag("Channel", meta.channel) \
            .field("voltage", voltage)
        self.write_point(point, bucket=meta.serial)

    def write_temperature(self, meta: MetaInfo, temperature: float):
        point = Point("temperature_measurement") \
            .tag("Channel", meta.channel) \
            .field("temperature", temperature)
        self.write_point(point, bucket=meta.serial)

    def write_current(self, meta: MetaInfo, current: float):
        point = Point("current_measurement") \
            .tag("Channel", meta.channel) \
            .field("current", current)
        self.write_point(point, bucket=meta.serial)

    def write_yield_today(self, meta: MetaInfo, yield_today: float):
        point = Point("yield_measurement") \
            .tag("Channel", meta.channel) \
            .field("yield", yield_today)
        self.write_point(point, bucket=meta.serial)

    def write_point(self, point: Point, bucket: str):
        write_api = self.write_client.write_api(write_options=ASYNCHRONOUS)
        write_api.write(bucket=bucket, record=point)
