from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import GlobalMonthlyWeatherClusters, GlobalMonthlyWeatherTrends
from pdp.data.noaa.stations import SurfaceWeatherStations, StationClusters


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
    station_clusters = StationClusters()
    # monthly_weather = GlobalMonthlyWeather()
    monthly_weather_cluster = GlobalMonthlyWeatherClusters()
    monthly_weather_trends = GlobalMonthlyWeatherTrends("global_monthly_weather_cluster")
    # stations_qualified = TrendStationsQualified()
    # frontend = GlobalMonthlyWeatherTrendsFrontend()
