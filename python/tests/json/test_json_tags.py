import pytest
import rkp.json


def test_bytes_and_structured_mapping_keys_round_trip() -> None:
    value = {(1, 2): "tuple", b"key": b"bytes", 3: "integer"}

    assert rkp.json.loads(rkp.json.dumps(value)) == value


def test_tag_shaped_user_dictionary_round_trips_without_becoming_bytes() -> None:
    value = {"__rkp_type__": "bytes", "data": "YWJj"}

    assert rkp.json.loads(rkp.json.dumps(value)) == value


def test_invalid_byte_tag_remains_an_ordinary_dictionary() -> None:
    text = '{"__rkp_type__": "bytes", "data": "not base64!"}'

    assert rkp.json.loads(text) == {
        "__rkp_type__": "bytes",
        "data": "not base64!",
    }


def test_scalar_mapping_keys_keep_their_python_types() -> None:
    value = {None: "none", False: "bool", 2.5: "float", 1: "integer", "1": "text"}

    assert rkp.json.loads(rkp.json.dumps(value)) == value


def test_nan_is_rejected_by_default_but_can_be_enabled() -> None:
    with pytest.raises(ValueError, match="Out of range float"):
        rkp.json.dumps(float("nan"))

    assert rkp.json.dumps(float("nan"), allow_nan=True) == "NaN"


def test_bytes_and_bytearray_documents_honor_encoding() -> None:
    document = '{"name": "René"}'.encode("utf-16")

    assert rkp.json.loads(document, encoding="utf-16") == {"name": "René"}
    assert rkp.json.loads(bytearray(document), encoding="utf-16") == {"name": "René"}
