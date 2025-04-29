
import os
import re
from typing import Iterator

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import Row, SparkSession, Column
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.window import Window

from pdp.data.data import DataSet, DataTable
from pdp.data.job import SparkTask


class ParseGlobalSummaryOfMonth(SparkTask):

    def __init__(self, data_folder_path: str):
        super().__init__('parse-gsom')
        self.data_folder_path = data_folder_path

    def read(self, spark: SparkSession) -> DataSet:

        read_files = [Row(file=f'{self.data_folder_path}/{f}', ghcn_id=f[:-4])
                      for f in os.listdir(self.data_folder_path)
                      if f.endswith(".csv")]

        df = (
            spark.createDataFrame(data=read_files)
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


    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

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


class GSOMByStation(SparkTask):
    MEASUREMENT_COLUMNS = {
        "TAVG": ("average_daily_temperature", "decimal(16,3)", "a,S"),
        "TMAX": ("average_daily_max_temperature", "decimal(16,3)", "a,S"),
        "TMIN": ("average_daily_min_temperature", "decimal(16,3)", "a,S"),
        "ADPT": ("average_dew_point_temperature", "decimal(16,3)", "a,M,Q,S"),
        "AWBT": ("average_wet_bulb_temperature", "decimal(16,3)", "a,M,Q,S"),
        "EMNT": ("extreme_minimum_temperature", "decimal(16,3)", "a,S,cc,d"),
        "EMXT": ("extreme_maximum_temperature", "decimal(16,3)", "a,S,cc,d"),
        "DT00": ("days_below_zero", "int", "a,S"),
        "DT32": ("days_below_freezing", "int", "a,S"),
        "DT70": ("days_above_70", "int", "a,S"),
        "DT90": ("days_above_90", "int", "a,S"),
        "CLDD": ("days_cooling_degree", "int", "a,S"),
        "CDSD": ("days_cooling_degree_season", "int", "a,S"),
        "HTDD": ("days_heating_degree", "int", "a,S"),
        "HDSD": ("days_heating_degree_season", "int", "a,S"),
        "ASLP": ("average_sea_level_pressure", "decimal(16,3)", "a,M,Q,S"),
        "ASTP": ("average_station_level_pressure", "decimal(16,3)", "a,M,Q,S"),
        "AWND": ("average_wind_speed", "decimal(16,3)", "a,S"),
        "WDF1": ("wind_dir_fast_minute", "decimal(16,3)", "a,S"),
        "WSF1": ("wind_speed_fast_minute", "decimal(16,3)", "a,S"),
        "WDF2": ("wind_dir_fast_two_minute", "decimal(16,3)", "a,S"),
        "WSF2": ("wind_speed_fast_two_minute", "decimal(16,3)", "a,S"),
        "WDF5": ("wind_dir_fast_five_second", "decimal(16,3)", "a,S"),
        "WSF5": ("wind_speed_fast_five_second", "decimal(16,3)", "a,S"),
        "WSFM": ("max_wind_speed", "decimal(16,3)", "a,S"),
        "WSFG": ("max_gust_speed", "decimal(16,3)", "a,S"),
        "RHAV": ("average_relative_humidity", "decimal(16,3)", "a,M,Q,S"),
        "RHMX": ("average_max_relative_humidity", "decimal(16,3)", "a,M,Q,S"),
        "RHMN": ("average_min_relative_humidity", "decimal(16,3)", "a,M,Q,S"),
        "PSUN": ("average_daily_pct_sunshine", "decimal(16,3)", "a,S"),
        "EMXP": ("max_daily_precipitation", "decimal(16,3)", "a,M,S,cc,d"),
        "EMSN": ("max_daily_snowfall", "decimal(16,3)", "a,M,S,cc,d"),
        "EMSD": ("max_daily_snow_depth", "decimal(16,3)", "a,M,S,cc,d"),
        "EVAP": ("total_evaporation", "decimal(16,3)", "a,M,Q,S"),
        "PRCP": ("total_precipitation", "decimal(16,3)", "a,M,Q,S"),
        "SNOW": ("total_snowfall", "decimal(16,3)", "a,M,Q,S"),
        "DP01": ("days_with_low_precipitation", "int", "a,S"),
        "DP10": ("days_with_high_precipitation", "int", "a,S"),
        "DSND": ("days_with_snow_depth", "int", "a,S"),
        "DSNW": ("days_with_snowfall", "int", "a,S"),
        "DYFG": ("days_with_fog", "int"),
        "DYHF": ("days_with_heavy_fog", "int"),
        "DYTS": ("days_with_thunderstorm", "int")
    }

    def __init__(self):
        super().__init__("monthly-station")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", "raw_monthly")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

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
              for k, v in GSOMByStation.MEASUREMENT_COLUMNS.items() if k in raw_columns
          ]

        measurements = raw.df.select(select_measurements)

        transformed = DataSet([
            DataTable("noaa", "monthly_by_station", measurements, "overwrite"),
        ])

        return transformed


class GSOMStationGroups(SparkTask):

    def __init__(self):
        super().__init__("gsom-station-groups")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", "station_groups"),
            DataTable("noaa", "monthly_by_station"),
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        measurements = read_data.get_table("monthly_by_station").df
        measurement_columns = list(measurements.columns)
        remove_columns = ["ghcn_id", "month_id", "date_month_start", "year", "month",
                          "date_of_extreme_minimum", "date_of_extreme_maximum",
                          "date_of_max_snow_depth", "date_of_max_snowfall"]
        for c in remove_columns:
            measurement_columns.remove(c)

        station_groups = (
            read_data.get_table("station_groups").df
            .select(
                "group_id",
                F.explode("station_ids").alias("ghcn_id")
            )
        )

        group_averages = (
            station_groups
            .join(read_data.get_table("monthly_by_station").df, "ghcn_id")
            .groupby("group_id", "month_id")
            .agg(*[c for m in measurement_columns for c in [
                    F.avg(m).alias(m),
                    F.count(m).alias(f'{m}_count'),
                    F.std(m).alias(f'{m}_std'),
                    F.stddev(m).alias(f'{m}_stddev')
                ]
            ])
            .withColumn("year", F.left("month_id", F.lit(4)).cast("int"))
            .withColumn("month", F.right("month_id", F.lit(2)).cast("int"))
        )

        return DataSet([
            DataTable("noaa", "monthly_by_group", group_averages, "overwrite")
        ])


class GlobalMonthlyWeatherTrends(SparkTask):
    TREND_N_YEARS = [5, 10, 25]

    SUPPORTED_TABLES = {
        "monthly_by_station": {"mode": "station", "key": "ghcn_id"},
        "monthly_by_group": {"mode": "station_group", "key": "group_id"}
    }

    def __init__(self, read_table: str):
        if read_table not in self.SUPPORTED_TABLES:
            raise Exception(f"Table {read_table} is not supported")
        self.read_table = read_table
        self.mode = self.SUPPORTED_TABLES[read_table]["mode"]
        self.key = self.SUPPORTED_TABLES[read_table]["key"]
        super().__init__(f"monthly-weather-{self.mode}-trends")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", self.read_table),
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        measurements = (
            read_data.get_table(self.read_table).df
            .withColumn("has_data", F.lit(True))
        )

        station_range: DataFrame = measurements.select(self.key).distinct()
        years_range: DataFrame = spark.createDataFrame(data=[Row(year=y) for y in range(1850, 2025)])
        months_range: DataFrame = spark.createDataFrame(data=[Row(month=m) for m in range(1, 13)])
        full_data_range: DataFrame = station_range.crossJoin(years_range).crossJoin(months_range)

        measurement_column_names = [v[0]
                                    for v in GSOMByStation.MEASUREMENT_COLUMNS.values()
                                    if v[0] in set(measurements.columns)]

        # Start `global_monthly_weather_trends` table by calculating rolling
        # averages of n past years for each station and month
        partition_by = [self.key, "month"]
        grouping_window = Window().partitionBy(partition_by).orderBy("year")
        trend_windows = {n: grouping_window.rowsBetween(-(n-1), 0) for n in self.TREND_N_YEARS}

        trend_columns = (
            [F.col(self.key), F.col("month_id"), F.col("year"), F.col("month")] +
            [
                F.when(F.count(c).over(w) == F.lit(n), F.avg(c).over(w).cast("decimal(16,3)")).alias(f"{c}_avg{n}")
                for c in measurement_column_names
                for n, w in trend_windows.items()
            ]
        )

        trends = (
            full_data_range
            .join(measurements, [self.key, "year", "month"], "left")
            .select(trend_columns)
        )

        # TODO add linear regressions to trends

        return DataSet([
            DataTable("noaa", f"{self.read_table}_trends", trends, "overwrite")
        ])


class GlobalMonthlyWeatherTrendsFrontend(SparkTask):
    ROLLING_N_YEARS = [25]

    def __init__(self):
        super().__init__("weather-frontend")

    def read(self, spark: SparkSession) -> DataSet:
        return DataSet([
            DataTable("noaa", "monthly_by_group"),
            DataTable("noaa", "monthly_by_group_trends")
        ])

    def transform(self, spark: SparkSession, read_data: DataSet) -> DataSet:

        measurements = read_data.get_table("monthly_by_group").df
        trends = read_data.get_table("monthly_by_group_trends").df

        base_data = (
            trends
            .join(measurements, ["group_id", "month", "year"], "left")
            .groupBy("group_id", "month")
        )

        collect_columns = ['year'] + [
            f'{c[0]}{suffix}'
            for c in GSOMByStation.MEASUREMENT_COLUMNS.values()
            if c[0] in measurements.columns
            for suffix in [''] + [f'_avg{y}' for y in self.ROLLING_N_YEARS]
        ]

        print(f"collect columns: {collect_columns}")

        def get_data_field(field_name: str) -> Column:
            field_array = F.col("data").getField(field_name)
            null_empty_array = F.when(
                F.array_size(F.array_compact(field_array)) > F.lit(0),
                F.to_json(field_array)
            ).otherwise(F.lit(None))
            return null_empty_array

        frontend = (
            base_data
            .agg(F.collect_list(F.struct(*collect_columns)).alias('data'))
            .withColumn("data", F.sort_array("data"))
            .select(
                [F.col("group_id"), F.col("month")] + [get_data_field(c).alias(c) for c in collect_columns]
            )
        )

        return DataSet([
            DataTable("weather", "monthly_trends", frontend, "overwrite")
        ])

    def write(self, write_dataset: DataSet):
        write_dataset.write_all_jdbc()

        # TODO Remove once array format is confirmed to work
        # frontend = (
        #     base_data
        #     .agg(F.collect_list(F.struct(*collect_columns)).alias('data'))
        #     .withColumn("data", F.sort_array("data"))
        #     .select(
        #         F.col("ghcn_id"),
        #         F.col("month"),
        #         F.to_json(
        #             F.struct(*[F.col(f"data").getField(c).alias(c) for c in collect_columns])
        #         ).alias("json"),
        #         F.to_json(
        #             F.struct(*[
        #                 F.arrays_zip(
        #                 F.col("data").getField("year"),
        #                     F.col(f"data").getField(c)
        #                 ).alias(c) for c in collect_columns])
        #         ).alias("json_2")
        #     )
        #     .withColumn("json_2", F.regexp_replace("json_2", '"0":','"x":'))
        #     .withColumn("json_2", F.regexp_replace("json_2", '"1":', '"y":'))
        # )
