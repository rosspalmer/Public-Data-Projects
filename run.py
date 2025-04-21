from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import GlobalMonthlyWeatherClusters, GlobalMonthlyWeatherTrends, GlobalMonthlyWeatherTrendsFrontend
from pdp.data.noaa.stations import SurfaceWeatherStations, StationClusters, StationClustersFrontend


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    # stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
    # station_clusters = StationClusters()
    # station_jdbc = StationClustersFrontend()
    # monthly_weather = GlobalMonthlyWeather()
    # monthly_weather_cluster = GlobalMonthlyWeatherClusters()
    # monthly_weather_trends = GlobalMonthlyWeatherTrends("global_monthly_weather_cluster")
    frontend = GlobalMonthlyWeatherTrendsFrontend()
