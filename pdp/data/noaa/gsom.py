
import os
from typing import Iterator

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkJob


class ParseGlobalSummaryOfMonth(SparkJob):

    def __init__(self, data_folder_path: str):
        super().__init__("raw_global_summary_of_month")
        self.data_folder_path = data_folder_path

    def read(self) -> DataSet:

        read_files = [Row(file=f'{self.data_folder_path}/{f}', ghcn_id=f[:-4])
                      for f in os.listdir(self.data_folder_path)
                      if f.endswith(".csv")]

        df = (
            self.spark.createDataFrame(data=read_files)
            .repartition(50)
        )

        def read_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
            for d in iterator:
                d["data"] = d["file"].apply(lambda x: pd.read_csv(x).to_json(None, "records"))
                yield d

        df = df.mapInPandas(read_batch, "ghcn_id string, file string, data string")

        return DataSet([
            DataTable("noaa", "pandas_read", df)
        ])


    def transform(self, read_data: DataSet) -> DataSet:

        df = read_data.get_table("pandas_read").df.persist()

        df = (
            df
            .withColumn("data", F.from_json("data", "array<map<string, string>>"))
            .select("ghcn_id", "file", F.explode("data").alias("data"))
            .persist()
        )

        headers = [
            r['header']
            for r in df.select(F.explode(F.map_keys("data")).alias("header")).distinct().collect()
        ]

        print(f"Found headers: {headers.sort()}")

        for header in headers:
            df = df.withColumn(header, F.element_at("data", header))

        db = DataSet([
            DataTable("noaa", "raw_monthly", df, "overwrite")
        ])

        return db


class GlobalMonthlyWeather(SparkJob):
    MEASUREMENT_COLUMNS = {
        "TAVG": ("average_daily_temperature", "decimal<16,3>", "a,S"),
        "TMAX": ("average_daily_max_temperature", "decimal<16,3>", "a,S"),
        "TMIN": ("average_daily_min_temperature", "decimal<16,3>", "a,S"),
        "ADPT": ("average_dew_point_temperature", "decimal<16,3>", "a,M,Q,S"),
        "AWBT": ("average_wet_bulb_temperature", "decimal<16,3>", "a,M,Q,S"),
        "EMNT": ("extreme_minimum_temperature", "decimal<16,3>", "a,S,cc,d"),
        "EMXT": ("extreme_maximum_temperature", "decimal<16,3>", "a,S,cc,d"),
        "ASLP": ("average_sea_level_pressure", "decimal<16,3>", "a,M,Q,S"),
        "ASTP": ("average_station_level_pressure", "decimal<16,3>", "a,M,Q,S"),
        "AWND": ("average_wind_speed", "decimal<16,3>", "a,S"),
        "RHAV": ("average_relative_humidity", "decimal<16,3>", "a,M,Q,S"),
        "RHMX": ("average_max_relative_humidity", "decimal<16,3>", "a,M,Q,S"),
        "RHMN": ("average_min_relative_humidity", "decimal<16,3>", "a,M,Q,S"),
        "PSUN": ("average_daily_pct_sunshine", "decimal<16,3>", "a,S"),
        "EMXP": ("max_daily_precipitation", "decimal<16,3>", "a,M,S,cc,d"),
        "EMSN": ("max_daily_snowfall", "decimal<16,3>", "a,M,S,cc,d"),
        "EMSD": ("max_daily_snow_depth", "decimal<16,3>", "a,M,S,cc,d"),
        "EVAP": ("total_evaporation", "decimal<16,3>", "a,M,Q,S"),
        "PRCP": ("total_precipitation", "decimal<16,3>", "a,M,Q,S"),
        "SNOW": ("total_snowfall", "decimal<16,3>", "a,M,Q,S"),
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

    def __init__(self):
        super().__init__("global_monthly_weather")

    def read(self) -> DataSet:
        return DataSet([
            DataTable("noaa", "raw_monthly")
        ])

    def transform(self, read_data: DataSet) -> DataSet:

        raw = read_data.get_table("raw_monthly")

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

        raw_columns = set(raw.df.columns)

        select_measurements = [
              F.col(k).alias(v) for k, v in id_columns.items()
          ] + [
              F.col("DATE").substr(0, 4).cast("int").alias("year"),
              F.col("DATE").substr(6, 2).cast("int").alias("month")
          ] + [
              # Convert to date type using formatting specified above
              F.to_date(F.col(k), v[1]).alias(v[0])
              for k, v in date_columns.items() if k in raw_columns
          ] + [
              # Convert to type defined in section above and use long form name
              F.col(k).cast(v[1]).alias(v[0])
              for k, v in GlobalMonthlyWeather.MEASUREMENT_COLUMNS.items() if k in raw_columns
          ]

        measurements = raw.df.select(select_measurements)

        transformed = DataSet([
            DataTable("noaa", "global_monthly_weather", measurements, "overwrite"),
        ])

        return transformed


class GlobalMonthlyWeatherTrends(SparkJob):

    def __init__(self):
        super().__init__("global_monthly_weather_trends")

    def read(self) -> DataSet:
        return DataSet([
            DataTable("noaa", "global_monthly_weather"),
        ])

    def transform(self, read_data: DataSet) -> DataSet:

        measurements = (
            read_data
            .get_table("global_monthly_weather")
            .df
            .withColumn("has_data", F.lit(True))
        )

        station_range: DataFrame = measurements.select("ghcn_id").distinct()
        years_range: DataFrame = self.spark.createDataFrame(data=[Row(year=y) for y in range(1900, 2025)])
        months_range: DataFrame = self.spark.createDataFrame(data=[Row(month=m) for m in range(1, 13)])
        full_data_range: DataFrame = station_range.crossJoin(years_range).crossJoin(months_range)

        measurement_column_names = [v[0]
                                    for v in GlobalMonthlyWeather.MEASUREMENT_COLUMNS.values()
                                    if v[0] in set(measurements.columns)]

        # Start `global_monthly_weather_trends` table by calculating rolling
        # averages of n past years for each station and month
        past_n_averages =  [3, 5, 10, 20]
        grouping_window = Window().partitionBy("ghcn_id", "month").orderBy("year")
        trend_windows = {n: grouping_window.rowsBetween(-(n-1), 0) for n in past_n_averages}

        trend_columns = ([F.col("ghcn_id"), F.col("month_id"), F.col("date_month_start"),
                         F.col("year"), F.col("month")] +
        [
            F.when(F.count(c).over(w) == F.lit(n), F.avg(c).over(w)).alias(f"avg{n}_{c}")
            for c in measurement_column_names
            for n, w in trend_windows.items()
        ])

        trends = (
            full_data_range
            .join(measurements, ["ghcn_id", "year", "month"], "left")
            .select(trend_columns)
        )

        # TODO add linear regressions to trends

        return DataSet([
            DataTable("noaa", "global_monthly_weather_trends", trends, "overwrite")
        ])


class TrendStationsQualified(SparkJob):

    def __init__(self):
        super().__init__("trend_stations_qualified")

    def read(self) -> DataSet:
        return DataSet([
            DataTable("noaa", "global_monthly_weather_trends")
        ])

    def transform(self, read_data: DataSet) -> DataSet:

        trends = read_data.get_table("global_monthly_weather_trends").df

        qualified = (
            trends
            .groupBy("ghcn_id")
            .agg(
                F.count("avg10_average_daily_temperature").alias("avg10_temperature_count"),
                F.count("avg10_total_precipitation").alias("avg10_precipitation_count"),
            )
        )

        write = [
            DataTable("noaa", "global_stations_trend_counts", qualified, "overwrite")
        ]

        return DataSet(write)


class GlobalMonthlyWeatherTrendsFrontend(SparkJob):

    def __init__(self):
        super().__init__("global_monthly_weather_trends_frontend")

    def read(self) -> DataSet:
        return DataSet([
            DataTable("noaa", "global_stations_trend_counts"),
            DataTable("noaa", "global_monthly_weather"),
            DataTable("noaa", "global_monthly_weather_trends")
        ])

    def transform(self, read_data: DataSet) -> DataSet:

        stations = read_data.get_table("global_stations_trend_counts").df
        measurements = read_data.get_table("global_monthly_weather").df
        trends = read_data.get_table("global_monthly_weather_trends").df

        stations = stations.filter("avg10_temperature_count >= 800").select("ghcn_id")

        collect_columns = ["year", "average_daily_temperature", "avg10_average_daily_temperature",
                           "average_daily_min_temperature", "avg10_average_daily_min_temperature",
                           "average_daily_max_temperature", "avg10_average_daily_max_temperature",
                           "total_precipitation", "avg10_total_precipitation"]

        frontend = (
            trends
            .join(stations, "ghcn_id", "inner")
            .join(measurements, ["ghcn_id", "year", "month"], "left")
            .orderBy("year")
            .groupBy("ghcn_id", "month")
            .agg(*[F.collect_list(c).alias(c) for c in collect_columns])
            .select(
                "ghcn_id",
                "month",
                F.to_json(F.struct(*collect_columns)).alias("json")
            )
        )

        return DataSet([
            DataTable("noaa", "global_monthly_trends_frontend", frontend, "overwrite")
        ])
