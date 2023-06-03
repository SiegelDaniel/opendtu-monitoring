import attr
from pydantic import BaseModel


class MetaInfo(BaseModel):
    channel: int
    serial: str
