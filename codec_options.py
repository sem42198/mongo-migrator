from bson.decimal128 import Decimal128
from bson.codec_options import TypeCodec
from bson.codec_options import TypeRegistry
from bson.codec_options import CodecOptions
from decimal import Decimal
import datetime

class DecimalCodec(TypeCodec):
    python_type = Decimal
    bson_type = Decimal128
    def transform_python(self, value):
        return Decimal128(value)
    def transform_bson(self, value):
        return value.to_decimal()

class DateCodec(TypeCodec):
    python_type = datetime.date
    bson_type = datetime.datetime
    def transform_python(self, value):
        return datetime.datetime.combine(value, datetime.time(0, 0, 0, 0))
    def transform_bson(self, value):
        return value.date()

decimal_codec = DecimalCodec()
date_codec = DateCodec()
type_registry = TypeRegistry([decimal_codec, date_codec])
codec_options = CodecOptions(type_registry=type_registry)

def get():
	return codec_options