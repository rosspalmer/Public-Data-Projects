
from pydantic import BaseModel

class WeatherStation(BaseModel):
    ghcn_id: str
    network_id: str
    name: str
    lat: float
    long: float
    city: str
    country: str
    state: str = None


class WeatherStationGroup(BaseModel):
    group_id: str
    coordinates: tuple[float, float]
    stations: list[WeatherStation]
    distance: float = 0.0


class WeatherMonthlyTrends(BaseModel):
    cluster_id: str
    network_id: str
    month: int
    years: list[int]
    data: dict[str, list[float | int]]
