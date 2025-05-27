from pdp.data.job import SparkJob
from pdp.data.weather.areas import WeatherAreas
from pdp.data.weather.ghcnd import GHCNDailyByStation
from pdp.data.weather.seasons import WeatherSeasonByArea, WeatherSeasonByStation, WeatherSeasonTrends
from pdp.data.weather.stations import SurfaceWeatherStation


with SparkJob("weather") as job:

    # stations = SurfaceWeatherStation("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd")
    # areas = WeatherAreas()

    # daily_parse = GHCNDParseTextFiles("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd/ghcnd_all")
    # daily_transformed = GHCNDailyByStation()
    # season_station = WeatherSeasonByStation()
    # season_area = WeatherSeasonByArea()
    season_station_trends = WeatherSeasonTrends("station")
    season_area_trends = WeatherSeasonTrends("area")
