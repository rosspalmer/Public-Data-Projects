from pdp.data.job import SparkJob
from pdp.data.noaa.gsom import GlobalMonthlyWeatherCity
from pdp.data.noaa.stations import SurfaceWeatherStations, CityWeatherStations


with SparkJob("big-homes") as job:
    # parse = ParseGlobalSummaryOfMonth("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
    stations = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
    city_stations = CityWeatherStations()
    # monthly_weather = GlobalMonthlyWeather()
    monthly_weather_city = GlobalMonthlyWeatherCity()
    # monthly_weather_trends = GlobalMonthlyWeatherTrends()
    # stations_qualified = TrendStationsQualified()
    # frontend = GlobalMonthlyWeatherTrendsFrontend()
