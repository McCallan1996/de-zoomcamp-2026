"""DLT REST source for NYC taxi data.

Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api

Pages through the API (1000 records per page) until an empty page is returned,
then loads all records into DuckDB under the `nyc_taxi` dataset.
"""

from typing import Iterator, Dict, Any
from urllib.parse import urlencode
import urllib.request
import json
import dlt

_BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.source
def taxi_source(page_size: int = 1000):
    """DLT source that yields taxi rows from the paginated REST API."""

    @dlt.resource(write_disposition="replace")
    def taxi_rows() -> Iterator[Dict[str, Any]]:
        page = 1
        while True:
            url = _BASE_URL + "?" + urlencode({"page": page})
            with urllib.request.urlopen(url, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            if not data:
                break
            yield from data
            page += 1

    return taxi_rows


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi",
)

if __name__ == "__main__":
    info = taxi_pipeline.run(taxi_source())
    print(info)
