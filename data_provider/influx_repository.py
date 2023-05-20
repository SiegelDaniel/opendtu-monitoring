import influxdb_client
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from data_models.measurement import ACMeasurement, DCMeasurement

token = os.environ.get("INFLUXDB_TOKEN")
org = "siegel"
url = "http://localhost:8086"
write_client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)


class InfluxRepository:
    def __init__(self, org: str, token: str, url: str = "http://localhost:8086") -> None:
        self.write_client = InfluxDBClient(url=url, token=token, org=org)

    def write_ac_measurement(self, measurement: ACMeasurement, measurement_time: int = None):
        point = Point("ac_measurement").tag("input_number", "1") \
            .field("voltage", measurement.voltage) \
            .field("current", measurement.current) \
            .field("power", measurement.power) \
            .field("temperature", measurement.temperature) \
            .field("yieldTotal", measurement.yieldTotal) \
            .field("yieldDay", measurement.yieldDay)
        self.write_point(point, measurement_time)

    def write_dc_measurement(self, measurement: DCMeasurement, measurement_time: int = None):
        point = Point("dc_measurement").tag("input_number", str(measurement.input_number)) \
            .field("current", measurement.current) \
            .field("voltage", measurement.voltage) \
            .field("power", measurement.power) \
            .field("yieldDay", measurement.yieldDay) \
            .field("yieldTotal", measurement.yieldTotal)
        self.write_point(point, measurement_time)

    def write_point(self, point: Point, measurement_time: int = None):
        if measurement_time is not None:
            point.time(measurement_time)
        write_api = self.write_client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket="my-bucket", record=point)
        write_api.__del__()
