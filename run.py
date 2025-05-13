from pdp.data.job import SparkJob
from pdp.data.noaa.ghcnd import GHCNDParseTextFiles, GHCNDTransformedValues
from pdp.data.noaa.gsom import (GSOMByStation, GSOMStationGroups,
                                GlobalMonthlyWeatherTrends, GlobalMonthlyWeatherTrendsFrontend)
from pdp.data.noaa.stations import SurfaceWeatherStations, StationGroups, StationGroupsFrontend


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    # stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
    # station_clusters = StationGroups()
    # station_jdbc = StationGroupsFrontend()
    # gsom_by_station = GSOMByStation()
    # gsom_by_station_group = GSOMStationGroups()
    # monthly_weather_trends = GlobalMonthlyWeatherTrends("monthly_by_group")
    # frontend = GlobalMonthlyWeatherTrendsFrontend()
    daily_parse = GHCNDParseTextFiles("/mnt/lake-fs/raw/gov/noaa/ncei/ghcnd/ghcnd_all")
    daily_transformed = GHCNDTransformedValues()
