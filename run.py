from pdp.data.job import SparkJob
from pdp.data.weather.ghcnd import GHCNDParseTextFiles, GHCNDTransformedValues
from pdp.data.weather.gsom import (GSOMByStation, GSOMStationGroups,
                                   GlobalMonthlyWeatherTrends, GlobalMonthlyWeatherTrendsFrontend)
from pdp.data.weather.seasons import GlobalWeatherSeasonByStation
from pdp.data.weather.stations import SurfaceWeatherStations, StationGroups, StationGroupsFrontend


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/weather/ncei/gsom")
    # stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/weather/ncei")
    # station_clusters = StationGroups()
    # station_jdbc = StationGroupsFrontend()
    # gsom_by_station = GSOMByStation()
    # gsom_by_station_group = GSOMStationGroups()
    # monthly_weather_trends = GlobalMonthlyWeatherTrends("monthly_by_group")
    # frontend = GlobalMonthlyWeatherTrendsFrontend()
    daily_parse = GHCNDParseTextFiles("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd/ghcnd_all")
    daily_transformed = GHCNDTransformedValues()
    seasons = GlobalWeatherSeasonByStation()
