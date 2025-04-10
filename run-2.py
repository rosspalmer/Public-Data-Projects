from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import TrendStationsQualified, GlobalMonthlyWeatherTrendsFrontend, GlobalMonthlyWeather, \
    GlobalMonthlyWeatherTrends, ParseGlobalSummaryOfMonth

# from pdp.data.noaa.stations import SurfaceWeatherStations

with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    # stations = SurfaceWeatherStations("")
    # monthly_weather = GlobalMonthlyWeather()
    # monthly_weather_trends = GlobalMonthlyWeatherTrends()
    # stations_qualified = TrendStationsQualified()
    frontend = GlobalMonthlyWeatherTrendsFrontend()
