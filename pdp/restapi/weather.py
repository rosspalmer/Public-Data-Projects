from collections import defaultdict

import pandas as pd
from fastapi import FastAPI
from geopy.distance import geodesic
import mariadb

from pdp.models.weather import WeatherStation, WeatherStationGroup, WeatherMonthlyTrends

app = FastAPI()

cur = mariadb.connect(
    host="10.0.0.85",
    user="ross",
    password="p@sswor!",
    database="weather"
).cursor()


@app.get("/weather-station-groups/{lat},{long}")
def station_groups(lat: float, long: float, max_distance_km: float = -1.0) -> list[WeatherStationGroup]:

    LOOKUP_N_LIMIT: int = 10
    LOOKUP_DEGREE_LIMIT: float = 5.0

    group_candidate_sql = f"""
    SELECT group_id, center_lat, center_long
    FROM station_groups
    WHERE ABS({lat} - center_lat) < {LOOKUP_DEGREE_LIMIT}
        AND ABS({long} - center_long) < {LOOKUP_DEGREE_LIMIT}
        AND network_id = 'W'
    """

    cur.execute(group_candidate_sql)

    group_ids = []; coords_data = []
    for row in cur:
        group_ids.append(row[0])
        coords_data.append((row[1], row[2]))

    print('coord complete')

    def calculate_distance_km(coordinates: (float, float)) -> float:
        return geodesic(coordinates, (lat, long)).km

    coords = pd.DataFrame({'group_id': group_ids, 'coordinates': coords_data})
    coords["distance"] = coords["coordinates"].apply(calculate_distance_km)

    print('distance complete')

    if max_distance_km > 0:
        coords = coords[coords["distance"] <= max_distance_km]

    coords.sort_values("distance", ascending=True, inplace=True)
    if len(coords.index) > LOOKUP_N_LIMIT:
        coords = coords.head(LOOKUP_N_LIMIT)

    print('sort + filter complete')

    in_group_ids = "('" + "','".join(coords["group_id"].tolist()) + "')"

    stations_sql = f"""
    SELECT 
        cs.group_id,
        s.ghcn_id,
        s.network_id,
        s.name,
        s.lat,
        s.long,
        s.city,
        s.country,
        s.state
    FROM group_stations AS cs
    JOIN stations AS s 
    ON cs.ghcn_id = s.ghcn_id
    WHERE cs.group_id IN {in_group_ids}
    """

    cur.execute(stations_sql)

    print('execute stations complete')

    stations = defaultdict(list)
    for row in cur:
        weather_station = WeatherStation(
            ghcn_id=row[1],
            network_id=row[2],
            name=row[3],
            lat=row[4],
            long=row[5],
            city=row[6],
            country=row[7],
            state=row[8]
        )
        stations[row[0]].append(weather_station)

    print('stations complete')

    coords["stations"] = coords["group_id"].apply(lambda x: stations[x])
    output = [WeatherStationGroup(**d) for d in coords.to_dict("records")]

    return output


@app.get("/weather-monthly-trends/{mode}/{group_id}/{month}")
def weather_monthly_trends(mode: str, group_id: str, month: int) -> WeatherMonthlyTrends:

    MODE_COLUMNS = {
        "temperature": [
            "average_daily_temperature", "average_daily_temperature_avg25",
            "average_daily_min_temperature", "average_daily_min_temperature_avg25",
            "average_daily_max_temperature", "average_daily_max_temperature_avg25"
        ],
        "humidity": [
            "average_relative_humidity", "average_relative_humidity",
            "average_min_relative_humidity", "average_min_relative_humidity",
            "average_max_relative_humidity", "average_max_relative_humidity"
        ],
        "precipitation": [
            "total_evaporation",
            "total_precipitation",
            "total_snowfall",
            # TODO add days with precipitation fields
            "days_with_snowfall",
            "days_with_thunderstorm"
        ]
    }

    if mode not in MODE_COLUMNS:
        raise Exception(f"Mode {mode} is not listed in MODE_COLUMNS")
    mode_columns = MODE_COLUMNS[mode]

    trend_query = \
f"""
SELECT
    group_id,
    month,
    year,
    {',\n\t'.join(mode_columns)}
FROM monthly_trends
WHERE group_id = '{group_id}'
    AND month = {month}
"""

    print("Start query")
    print(trend_query)
    cur.execute(trend_query)
    print("End query")

    data = cur.next()

    trend_data = {
        "group_id": data[0],
        "month": data[1],
        "years": [int(y) for y in data[2][1:-1].split(',')],
        "data": {
            c: [float(d) if d != 'null' else None for d in data[3+i][1:-1].split(',')]
            for i, c in enumerate(mode_columns)
        }
    }

    output = WeatherMonthlyTrends(**trend_data)

    return output
