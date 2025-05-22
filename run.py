from pdp.data.job import SparkJob
from pdp.data.weather.areas import WeatherAreas
from pdp.data.weather.ghcnd import GHCNDailyByStation
from pdp.data.weather.seasons import GlobalWeatherSeasonByStation
from pdp.data.weather.stations import SurfaceWeatherStation


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/weather/ncei/gsom")
    stations = SurfaceWeatherStation("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd")
    areas = WeatherAreas()
    # daily_parse = GHCNDParseTextFiles("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd/ghcnd_all")
    # daily_transformed = GHCNDailyByStation()
    # seasons = GlobalWeatherSeasonByStation()
