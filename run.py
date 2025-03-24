from pdp.noaa.gsom import GlobalSummaryOfMonthParse
from pdp.noaa.stations import SurfaceWeatherStations

# job = SurfaceWeatherStations("/mnt/lake-fs/raw/gov/noaa/ncei")
job = GlobalSummaryOfMonthParse("/mnt/lake-fs/raw/gov/noaa/ncei/gsom")
job.run()
