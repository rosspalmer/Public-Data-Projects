
import pyspark.sql.functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window

from pdp.data import DataSet, DataTable
from pdp.job import SparkJob


class GlobalSummaryOfMonthParse(SparkJob):

    def __init__(self, data_folder_path: str):
        super().__init__("gsom-parse")
        self.data_folder_path = data_folder_path

    def read(self) -> DataSet:

        data_files = (
            self.spark
            .read
            .format("csv")
            .option("header", "true")
            .load(self.data_folder_path)
            .withColumn("filename", F.input_file_name())
            .repartition(50, "STATION")
            .persist()
        )

        db = DataSet([
            DataTable("raw_monthly", data_files, "noaa")
        ])

        return db

    def transform(self, data: DataSet) -> DataSet:

        raw = data.get_table("raw_monthly")

        id_columns = {
            "STATION": "ghcn_id",
            "DATE": "month_id"
        }

        date_columns = {
            "DATE": ("date_month_start", "yyyy-MM"),
            "DYNT": ("date_of_extreme_minimum", "yyyyMMdd", "a,S"),
            "DYXT": ("date_of_extreme_maximum", "yyyyMMdd", "a,S"),
            "DYSD": ("date_of_max_snow_depth", "yyyyMMdd", "a,S"),
            "DYSN": ("date_of_max_snowfall", "yyyyMMdd", "a,S")
        }

        measurement_columns = {
            "TAVG": ("average_daily_temperature", "float", "a,S"),
            "TMAX": ("average_daily_max_temperature", "float", "a,S"),
            "TMIN": ("average_daily_min_temperature", "float", "a,S"),
            "ADPT": ("average_dew_point_temperature", "float", "a,M,Q,S"),
            "AWBT": ("average_wet_bulb_temperature", "float", "a,M,Q,S"),
            "EMNT": ("extreme_minimum_temperature", "float", "a,S,cc,d"),
            "EMXT": ("extreme_maximum_temperature", "float", "a,S,cc,d"),
            "ASLP": ("average_sea_level_pressure", "float", "a,M,Q,S"),
            "ASTP": ("average_station_level_pressure", "float", "a,M,Q,S"),
            "AWND": ("average_wind_speed", "float", "a,S"),
            "RHAV": ("average_relative_humidity", "float", "a,M,Q,S"),
            "RHMX": ("average_max_relative_humidity", "float", "a,M,Q,S"),
            "RHMN": ("average_min_relative_humidity", "float", "a,M,Q,S"),
            "PSUN": ("average_daily_pct_sunshine", "float", "a,S"),
            "EMXP": ("max_daily_precipitation", "float", "a,M,S,cc,d"),
            "EMSN": ("max_daily_snowfall", "float", "a,M,S,cc,d"),
            "EMSD": ("max_daily_snow_depth", "float", "a,M,S,cc,d"),
            "EVAP": ("total_evaporation", "float", "a,M,Q,S"),
            "PRCP": ("total_precipitation", "float", "a,M,Q,S"),
            "SNOW": ("total_snowfall", "float", "a,M,Q,S"),
            "DSND": ("days_with_snow_depth", "int", "a,S"),
            "DSNW": ("days_with_snowfall", "int", "a,S"),
            "DT00": ("days_below_zero", "int", "a,S"),
            "DT32": ("days_below_freezing", "int", "a,S"),
            "DT70": ("days_above_70", "int", "a,S"),
            "DT90": ("days_above_90", "int", "a,S"),
            "CDSD": ("cooling_degree_days_season", "int", "a,S"),
            "CLDD": ("cooling_degree_days", "int", "a,S"),
            "HDSD": ("heating_degree_days_season", "int", "a,S"),
            "HTDD": ("heating_degree_days", "int", "a,S"),
            "DYFG": ("days_with_fog", "int"),
            "DYHF": ("days_with_heavy_fog", "int"),
            "DYTS": ("days_with_thunderstorm", "int")
        }

        raw_columns = set(raw.df.columns)
        measurement_column_names = [v[0] for k,v in measurement_columns.items() if k in raw_columns]

        select_measurements = [
            F.col(k).alias(v) for k, v in id_columns.items()
        ] + [
            F.col("DATE").substr(0, 4).alias("year"),
            F.col("DATE").substr(6, 2).alias("month")
        ] + [
            # Convert to date type using formatting specified above
            F.to_date(F.col(k), v[1]).alias(v[0])
            for k, v in date_columns.items() if k in raw_columns
        ] + [
            # Convert to type defined in section above and use long form name
            F.col(k).cast(v[1]).alias(v[0])
            for k, v in measurement_columns.items() if k in raw_columns
        ]

        measurements = raw.df.select(select_measurements).persist()

        years_lookback =  [3, 5, 10]
        grouping_window = Window().partitionBy("ghcn_id", "month").orderBy("year")
        trend_windows = {n: grouping_window.rowsBetween(-(n-1), 0) for n in years_lookback}
        trend_columns = {
            n: [F.col("ghcn_id"), F.col("month_id"), F.col("date_month_start")] +
               [F.avg(c).over(w).alias(c) for c in measurement_column_names]
            for n, w in trend_windows.items()
        }

        trend_tables = [
            DataTable(f'global_monthly_weather_avg_{n}', measurements.select(cols), "noaa")
            for n, cols in trend_columns.items()
        ]

        transformed = DataSet([
            DataTable("global_monthly_weather", measurements, "noaa"),
            # TODO add measurement attributes table
        ] + trend_tables)

        return transformed

    def write(self, data: DataSet):
        data.write_all_tables("overwrite")
