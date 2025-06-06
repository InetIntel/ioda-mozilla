import logging

import pytest

from constants import GCP_PROJECT_ID
from mozillaScraper import fetchData

unknown_region = "ABC"


def test_mozilla_region_not_found(caplog):
    with caplog.at_level(logging.ERROR):
        result = fetchData(GCP_PROJECT_ID, None, None, region='ABC', saved={})

    assert result == -1
    assert any("Region ABC not found in Mozilla Telemetry data." in message for message in caplog.text.splitlines())

