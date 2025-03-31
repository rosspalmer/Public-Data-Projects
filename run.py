from pdp.noaa.gsom import GlobalSummaryOfMonthParse
from pdp.noaa.stations import SurfaceWeatherStations

print("stations")
job = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
job.run()

print("gsom-parse")
job = GlobalSummaryOfMonthParse("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
job.run()
