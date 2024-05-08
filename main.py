import argparse
import datetime
import sqlite3
from pathlib import Path
from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest, DwdMosmixType

PARAMETERS = {
    'temperature_air_mean_200': 0,
    'precipitation_height_significant_weather_last_1h': 1
}


def log_forecasts(database_file: Path, station_ids: list[str]):
    db = sqlite3.Connection(database_file)

    cursor = db.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS forecasts (
        recorded_at INTEGER NOT NULL,
        station_id INTEGER NOT NULL,
        forecast_time INTEGER NOT NULL,
        forecast_type INTEGER NOT NULL,
        value REAL NOT NULL
    );
    """)

    db.commit()

    request = DwdMosmixRequest(
        parameter=["ttt", "rr1c"],
        mosmix_type=DwdMosmixType.SMALL,
    )

    stations = request.filter_by_station_id(station_ids)
    recorded_at = datetime.datetime.now()

    for result in stations.values.query():
        rows = [
            (recorded_at, station_id, date, PARAMETERS[parameter], value)
            for (station_id, dataset, parameter, date, value, quality) in result.df.rows()
        ]
        cursor.executemany("""
        INSERT INTO forecasts(recorded_at, station_id, forecast_time, forecast_type, value) 
        VALUES (?, ?, ?, ?, ?)
        """, rows)

        db.commit()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database_file", type=Path)
    parser.add_argument("station_id", nargs="+", type=str)
    args = parser.parse_args()

    log_forecasts(args.database_file, args.station_id)
