from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import TrendStationsQualified, GlobalMonthlyWeatherTrendsFrontend, GlobalMonthlyWeather, \
    GlobalMonthlyWeatherTrends, ParseGlobalSummaryOfMonth

# from pdp.data.noaa.stations import SurfaceWeatherStations

with SparkJob("big-homes") as job:
    parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    # stations = SurfaceWeatherStations("")


# print("stations")
# job = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
# job.run()

#
# print("global-monthly-weather")
# job = GlobalMonthlyWeather()
# job.run()
