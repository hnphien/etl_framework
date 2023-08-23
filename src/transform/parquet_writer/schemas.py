"""define schemas"""
import pyarrow as pa

schema_color_table = pa.schema(
    [
        pa.field("color", pa.string()),
        pa.field("value", pa.string())
    ]
)
