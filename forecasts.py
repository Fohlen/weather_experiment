import argparse
import datetime
import json
from collections import namedtuple
import sys
import os
from typing import Iterable

from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest, DwdMosmixType
from google.cloud import bigquery
import functions_framework

_KEYS = ["recorded_at", "station_id", "forecast_time", "parameter", "value"]
Forecast = namedtuple("Forecast", _KEYS)


@functions_framework.cloud_event
def insert_to_bigquery(cloud_event):
    """Inserts a row into a BigQuery table.

    Args:
      cloud_event: (dict) Event payload from Cloud Function trigger.
    """

    station_ids = os.environ.get("STATION_IDS", "").split(",")
    print(f"Fetching forecasts for {len(station_ids)} stations")

    rows = list(retrieve_forecasts(station_ids))
    print(f"Inserting {len(rows)} forecasts")

    # Get the BigQuery client
    client = bigquery.Client()

    # Define your table reference (replace with your project, dataset and table)
    table_ref = client.get_table(os.environ["BIGQUERY_TABLE"])

    # Insert row into the table
    client.insert_rows(table_ref, [dict(zip(_KEYS, r)) for r in rows])

    print("Data inserted successfully!")


def retrieve_forecasts(station_ids: list[str]) -> Iterable[Forecast]:
    request = DwdMosmixRequest(
        parameter=["ttt", "rr1c"],
        mosmix_type=DwdMosmixType.SMALL,
    )

    stations = request.filter_by_station_id(station_ids)
    recorded_at = datetime.datetime.now()

    for result in stations.values.query():
        yield from [
            (recorded_at, station_id, date, parameter, value)
            for (station_id, dataset, parameter, date, value, quality) in result.df.rows()
        ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("station_id", nargs="+", type=str)
    args = parser.parse_args()

    print(f"Fetching forecasts for {len(args.station_id)} stations", file=sys.stderr)

    for row in retrieve_forecasts(args.station_id):
        print(
            json.dumps(dict(zip(_KEYS, row)), sort_keys=True, default=str),
            file=sys.stdout
        )
