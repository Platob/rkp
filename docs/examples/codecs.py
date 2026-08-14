"""Round-trip records through the built-in JSON and YAML codecs."""

from __future__ import annotations

from io import BytesIO, StringIO

from rkp import Record, json, record, yaml


@record
class Message(Record):
    identifier: int
    labels: list[str]
    note: str | None = None


def main() -> None:
    value = Message(1, ["docs", "local"])

    json_text = json.dumps(value, indent=2)
    yaml_text = yaml.dumps(value, sort_keys=False)
    assert json.loads(json_text, cls=Message) == value
    assert yaml.loads(yaml_text, cls=Message) == value

    # Decorated records expose format-specific and generic conveniences.
    assert Message.loads_json(value.dumps_json()) == value
    assert Message.loads_yaml(value.dumps_yaml()) == value
    assert Message.loads(value.dumps(), format="json") == value

    stream = StringIO()
    value.dump_json(stream)
    assert Message.loads_json(stream.getvalue()) == value

    binary = BytesIO()
    value.dump_yaml_bytes(binary)
    binary.seek(0)
    assert Message.load_yaml(binary) == value
    assert Message.loads_json(value.dumps_json_bytes()) == value

    # A string without '/' or '\\' is document text, not a path.
    assert Message.load('{"identifier": 2, "labels": []}', format="json") == Message(
        2, []
    )
    print(json_text)
    print(yaml_text)


if __name__ == "__main__":
    main()
