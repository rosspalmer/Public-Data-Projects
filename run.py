from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import (GSOMStations, GSOMStationGroups,
                                GlobalMonthlyWeatherTrends, GlobalMonthlyWeatherTrendsFrontend)
from pdp.data.noaa.stations import SurfaceWeatherStations, StationClusters, StationClustersFrontend


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    # stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
    # station_clusters = StationClusters()
    # station_jdbc = StationClustersFrontend()
    gsom_stations = GSOMStations()
    gsom_station_groups = GSOMStationGroups()
    monthly_weather_trends = GlobalMonthlyWeatherTrends("monthly_by_group")
    frontend = GlobalMonthlyWeatherTrendsFrontend()
