from __future__ import annotations

import importlib.metadata

import idc_index_data as m


def test_version():
    assert importlib.metadata.version("idc_index_data") == m.__version__
