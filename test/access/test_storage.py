"""
Tests the persistent storage implementation
"""

import pytest

import data_crawler.access.storage as storage


def test_storage_simple(redis_pool):
    """Tests a simple storage lifecycle"""

    st = storage.PersistentAPIStorage(redis_pool, "<test>")

    del st["val"]  # Just to make sure that no artefacts are in the storage
    assert "val" not in st

    st["val"] = "ue"
    assert "val" in st
    assert st["val"] == "ue"

    del st["val"]
    assert "val" not in st
    assert "val" not in st  # Make sure that the in operator does not create the key again


def test_storage_complex_value(redis_pool):
    """Tests storing a more complex calue"""

    st = storage.PersistentAPIStorage(redis_pool, "<test>")

    ref_value = {
        "A": None,
        "B": 123.0,
        "C": "HoHoHo"
    }

    del st["val"]  # Just to make sure that no artefacts are in the storage
    assert "val" not in st
    st["val"] = ref_value

    assert st["val"] == ref_value
    del st["val"]

