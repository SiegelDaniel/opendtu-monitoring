import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS

from data_models.measurement import VoltageMeasurement, TemperatureMeasurement, CurrentMeasurement, YieldMeasurement

token = os.environ.get("INFLUXDB_TOKEN")
org = "siegel"
url = "http://localhost:8086"


class InfluxRepository:
    def __init__(self, org: str, token: str, url: str = "http://localhost:8086") -> None:
        self.write_client = InfluxDBClient(url=url, token=token, org=org)
    message_buffer = []

    def write_voltage(self, voltage: VoltageMeasurement):
        point = Point("voltage_measurement")\
            .tag("Channel",voltage.meta.channel)\
            .field("voltage", voltage)
        self.message_buffer.append(point)

    def write_temperature(self, temperature: TemperatureMeasurement):
        point = Point("temperature_measurement") \
            .tag("Channel", temperature.meta.channel) \
            .field("temperature", temperature.value)
        self.message_buffer.append(point)

    def write_current(self, current: CurrentMeasurement):
        point = Point("current_measurement") \
            .tag("Channel", current.meta.channel) \
            .field("current", current.value)
        self.message_buffer.append(point)

    def write_yield_today(self, yield_today: YieldMeasurement):
        point = Point("yield_measurement") \
            .tag("Channel", yield_today.meta.channel) \
            .field("yield", yield_today.value)
        self.message_buffer.append(point)


    def write_point(self, point: Point, bucket: str):
        write_api = self.write_client.write_api(write_options=ASYNCHRONOUS)
        write_api.write(bucket=bucket, record=point)
