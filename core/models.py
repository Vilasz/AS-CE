from dataclasses import dataclass
import datetime

@dataclass
class MeteorologicalEvent:
    timestamp: datetime.datetime
    station_id: int
    region: str
    temperature: float
    humidity: float
    pressure: float