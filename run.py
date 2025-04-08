from pdp.data.noaa.gsom import TrendStationsQualified, GlobalMonthlyWeatherTrendsFrontend, GlobalMonthlyWeather, \
    GlobalMonthlyWeatherTrends
from pdp.data.noaa.stations import SurfaceWeatherStations

# print("stations")
# job = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
# job.run()
#
# print("gsom-parse")
# job = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
# job.run()
#
# print("global-monthly-weather")
# job = GlobalMonthlyWeather()
# job.run()
#
print("global-monthly-weather-trends")
job = GlobalMonthlyWeatherTrends()
job.run()

print("global-monthly-weather-trends")
job = TrendStationsQualified()
job.run()

print("frontend")
job = GlobalMonthlyWeatherTrendsFrontend()
job.run()