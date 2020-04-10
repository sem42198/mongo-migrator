from bson.decimal128 import Decimal128
from bson.codec_options import TypeCodec
from bson.codec_options import TypeRegistry
from bson.codec_options import CodecOptions
from decimal import Decimal

class DecimalCodec(TypeCodec):
    python_type = Decimal    # the Python type acted upon by this type codec
    bson_type = Decimal128   # the BSON type acted upon by this type codec
    def transform_python(self, value):
        """Function that transforms a custom type value into a type
        that BSON can encode."""
        return Decimal128(value)
    def transform_bson(self, value):
        """Function that transforms a vanilla BSON type value into our
        custom type."""
        return value.to_decimal()
decimal_codec = DecimalCodec()
type_registry = TypeRegistry([decimal_codec])
codec_options = CodecOptions(type_registry=type_registry)

def get():
	return codec_options