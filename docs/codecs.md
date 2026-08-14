# JSON and YAML

The built-in `rkp.json` and `rkp.yaml` modules share the record conversion
rules. YAML has no PyYAML dependency and parses one safe YAML 1.2 document.

```python
from rkp import Record, record
from rkp import json, yaml


@record
class Message(Record):
    identifier: int
    labels: list[str]


value = Message(1, ["docs", "example"])

json_text = json.dumps(value, indent=2)
yaml_text = yaml.dumps(value, sort_keys=False, explicit_start=True)
assert json.loads(json_text, cls=Message) == value
assert yaml.loads(yaml_text, cls=Message) == value
```

Both modules expose `load`, `loads`, `dump`, and `dumps`. `loads` and `dumps`
operate on document data. `load` and `dump` accept a path, a text/binary
stream, or a string buffer. Their `dumps_bytes` and `dump_bytes` counterparts
encode directly to bytes and write only to binary streams or paths.

```python
from io import BytesIO

payload = json.dumps_bytes(value)
assert json.loads(memoryview(payload), cls=Message) == value

buffer = BytesIO()
json.dump_bytes(value, buffer)
buffer.seek(0)
assert json.load(buffer, cls=Message) == value
```

Binary methods avoid text-stream probing and encode exactly once. The caller
owns a supplied stream, so it remains open and its current position is used.

## Paths versus string buffers

A string is treated as a path only when it contains `/` or `\\`. This keeps
short JSON/YAML documents unambiguous.

```python
from pathlib import Path
from rkp import json

# Document text: no path separator.
message = json.load('{"identifier": 2, "labels": []}', cls=Message)

# Explicit paths.
json.dump(message, Path("build/message.json"))
message = json.load("./build/message.json", cls=Message)

# A separator-free destination string behaves as an immutable buffer.
text = json.dump(message, "")
assert isinstance(text, str)
```

Use `Path("message.json")`, `"./message.json"`, or
`".\\message.json"` for a filename without another directory component.
Caller-owned streams remain open.

## Generated record methods

`@record(with_json=True, with_yaml=True)` installs format-specific methods and
generic dispatch:

```python
assert Message.loads_json(value.dumps_json()) == value
assert Message.loads_yaml(value.dumps_yaml()) == value

# Generic string methods prefer JSON when both codecs are enabled.
assert Message.loads(value.dumps()) == value

# Generic file methods infer .json, .yaml, or .yml from the target name.
value.dump("./build/message.yaml")
assert Message.load("./build/message.yaml") == value

# Byte-oriented variants use the same format inference.
value.dump_bytes("./build/message.json")
assert Message.load("./build/message.json") == value

wire = value.dumps_yaml_bytes()
assert Message.loads_yaml(wire) == value
```

Pass `format="json"` or `format="yaml"` for unnamed streams and whenever
explicit dispatch is clearer. Set `with_json=False` or `with_yaml=False` on the
decorator to disable that codec's generated methods.

The complete byte-returning record surface is `dumps_bytes`,
`dumps_json_bytes`, and `dumps_yaml_bytes`. The matching destination methods
are `dump_bytes`, `dump_json_bytes`, and `dump_yaml_bytes`. Existing `load`,
`load_json`, `load_yaml`, and their `loads*` forms accept `bytes`, `bytearray`,
`memoryview`, binary streams, and binary file paths directly.

`safe` and `on_error` on the load side have the same behavior as
`Record.from_dict()`. JSON accepts the standard library decoder/encoder keyword
arguments. YAML dumping accepts `sort_keys`, `indent`, `explicit_start`,
`explicit_end`, and `line_break`.

Run [`examples/codecs.py`](examples/codecs.py) without filesystem writes:

```console
uv run --project python python docs/examples/codecs.py
```
