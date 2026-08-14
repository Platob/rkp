"""Generate an RKP record from local FIX field definitions."""

from rkp.fix import FixDictionary, FixEnumValue, FixField

dictionary = FixDictionary(
    version="4.4",
    fields=(
        FixField(11, "ClOrdID", "String", "4.4"),
        FixField(
            54,
            "Side",
            "char",
            "4.4",
            description="Side of order.",
            values=(FixEnumValue("1", "Buy"), FixEnumValue("2", "Sell")),
        ),
    ),
)

NewOrder = dictionary.into_record("NewOrder", required=(11, 54))
order = NewOrder(cl_ord_id="client-1", side="1")

assert order.dumps_json() == '{"ClOrdID": "client-1", "Side": "1"}'
side = NewOrder.into_arrow_schema().field("Side")
assert side.metadata is not None
assert side.metadata[b"PARQUET:field_id"] == b"54"
assert side.metadata[b"fix.type"] == b"char"
