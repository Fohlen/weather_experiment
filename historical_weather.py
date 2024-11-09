import datetime as dt
import csv
import sys

from wetterdienst.provider.dwd.observation import DwdObservationRequest, DwdObservationDataset, DwdObservationResolution, DwdObservationParameter

station_mapping = {
    "10384": "00433",
    "10513": "02667",
    "10865": "03379",
    "10803": "01443",
    "10729": "05906",
    "10727": "02522",
    "10776": "04104",
    "10554": "01270",
    "10555": "05419"
}

writer = csv.writer(sys.stdout)
writer.writerow(["recorded_at","station_id","parameter","value"])

# request temperature data

request = DwdObservationRequest(
    parameter=[
        DwdObservationParameter.HOURLY.TEMPERATURE_AIR.TEMPERATURE_AIR_MEAN_2M,
        DwdObservationParameter.HOURLY.PRECIPITATION.PRECIPITATION_HEIGHT
    ],
    resolution=DwdObservationResolution.HOURLY,
    start_date=dt.datetime(2024, 5, 6),
    end_date=dt.datetime(2024, 11, 6)
)

stations = request.filter_by_station_id(station_id=station_mapping.values())

for result in stations.values.query():
    rows = [
        (date, station_id, parameter, value)
        for (station_id, dataset, parameter, date, value, quality) in result.df.rows()
    ]
    writer.writerows(rows)

# request precipitation data

