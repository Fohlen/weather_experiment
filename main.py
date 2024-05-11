import argparse
import datetime
import json
from collections import namedtuple
import sys

from wetterdienst.provider.dwd.mosmix import DwdMosmixRequest, DwdMosmixType

_KEYS = ["recorded_at", "station_id", "forecast_time", "parameter", "value"]
Forecast = namedtuple("Forecast", _KEYS)


def retrieve_forecasts(station_ids: list[str]):
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
    forecasts = retrieve_forecasts(args.station_id)

    for row in forecasts:
        print(
            json.dumps(dict(zip(_KEYS, row)), sort_keys=True, default=str),
            file=sys.stdout
        )
