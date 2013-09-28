# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from decimal import Decimal
import cql
from cql.apivalues import UUID
from cql.cqltypes import lookup_casstype

marshalled_value_pairs = (
    ('lorem ipsum dolor sit amet', 'AsciiType', 'lorem ipsum dolor sit amet'),
    ('', 'AsciiType', ''),
    ('\x01', 'BooleanType', True),
    ('\x00', 'BooleanType', False),
    ('', 'BooleanType', None),
    ('\xff\xfe\xfd\xfc\xfb', 'BytesType', '\xff\xfe\xfd\xfc\xfb'),
    ('', 'BytesType', ''),
    ('\x7f\xff\xff\xff\xff\xff\xff\xff', 'CounterColumnType',  9223372036854775807),
    ('\x80\x00\x00\x00\x00\x00\x00\x00', 'CounterColumnType', -9223372036854775808),
    ('', 'CounterColumnType', None),
    ('\x00\x00\x013\x7fb\xeey', 'DateType', 1320692149.881),
    ('', 'DateType', None),
    ('\x00\x00\x013\x7fb\xeey', 'TimestampType', 1320692149.881),
    ('', 'TimestampType', None),
    ('\x00\x00\x00\r\nJ\x04"^\x91\x04\x8a\xb1\x18\xfe', 'DecimalType', Decimal('1243878957943.1234124191998')),
    ('\x00\x00\x00\x06\xe5\xde]\x98Y', 'DecimalType', Decimal('-112233.441191')),
    ('\x00\x00\x00\x14\x00\xfa\xce', 'DecimalType', Decimal('0.00000000000000064206')),
    ('\x00\x00\x00\x14\xff\x052', 'DecimalType', Decimal('-0.00000000000000064206')),
    ('\xff\xff\xff\x9c\x00\xfa\xce', 'DecimalType', Decimal('64206e100')),
    ('\x00\x00\x00\x01\x00', 'DecimalType', Decimal('0.0')),
    ('', 'DecimalType', None),
    ('@\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', 19432.125),
    ('\xc0\xd2\xfa\x08\x00\x00\x00\x00', 'DoubleType', -19432.125),
    ('\x7f\xef\x00\x00\x00\x00\x00\x00', 'DoubleType', 1.7415152243978685e+308),
    ('', 'DoubleType', None),
    ('F\x97\xd0@', 'FloatType', 19432.125),
    ('\xc6\x97\xd0@', 'FloatType', -19432.125),
    ('\xc6\x97\xd0@', 'FloatType', -19432.125),
    ('\x7f\x7f\x00\x00', 'FloatType', 338953138925153547590470800371487866880.0),
    ('', 'FloatType', None),
    ('\x7f\x50\x00\x00', 'Int32Type', 2135949312),
    ('\xff\xfd\xcb\x91', 'Int32Type', -144495),
    ('', 'Int32Type', None),
    ('f\x1e\xfd\xf2\xe3\xb1\x9f|\x04_\x15', 'IntegerType', 123456789123456789123456789),
    ('\x00', 'IntegerType', 0),
    ('', 'IntegerType', None),
    ('\x7f\xff\xff\xff\xff\xff\xff\xff', 'LongType',  9223372036854775807),
    ('\x80\x00\x00\x00\x00\x00\x00\x00', 'LongType', -9223372036854775808),
    ('', 'LongType', None),
    ('', 'InetAddressType', None),
    ('A46\xa9', 'InetAddressType', '65.52.54.169'),
    ('*\x00\x13(\xe1\x02\xcc\xc0\x00\x00\x00\x00\x00\x00\x01"', 'InetAddressType', '2a00:1328:e102:ccc0::122'),
    ('\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6', 'UTF8Type', u'\u307e\u3057\u3066'),
    ('\xe3\x81\xbe\xe3\x81\x97\xe3\x81\xa6' * 1000, 'UTF8Type', u'\u307e\u3057\u3066' * 1000),
    ('', 'UTF8Type', u''),
    ('\xff' * 16, 'UUIDType', UUID('ffffffff-ffff-ffff-ffff-ffffffffffff')),
    ('I\x15~\xfc\xef<\x9d\xe3\x16\x98\xaf\x80\x1f\xb4\x0b*', 'UUIDType', UUID('49157efc-ef3c-9de3-1698-af801fb40b2a')),
    ('', 'UUIDType', None),
    ('', 'MapType(AsciiType, BooleanType)', None),
    ('', 'ListType(FloatType)', None),
    ('', 'SetType(LongType)', None),
    ('\x00\x00', 'MapType(DecimalType, BooleanType)', {}),
    ('\x00\x00', 'ListType(FloatType)', ()),
    ('\x00\x00', 'SetType(IntegerType)', set()),
    ('\x00\x01\x00\x10\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0', 'ListType(TimeUUIDType)', (UUID(bytes='\xafYC\xa3\xea<\x11\xe1\xabc\xc4,\x03"y\xf0'),)),
    # these following entries work for me right now, but they're dependent on
    # vagaries of internal python ordering for unordered types
    ('\x00\x03\x00\x06\xe3\x81\xbfbob\x00\x04\x00\x00\x00\xc7\x00\x00\x00\x04\xff\xff\xff\xff\x00\x01\\\x00\x04\x00\x00\x00\x00', 'MapType(UTF8Type, Int32Type)', {u'\u307fbob': 199, u'': -1, u'\\': 0}),
    ('\x00\x02\x00\x08@\x14\x00\x00\x00\x00\x00\x00\x00\x08@\x01\x99\x99\x99\x99\x99\x9a', 'SetType(DoubleType)', set([2.2, 5.0])),
)

class TestUnmarshal(unittest.TestCase):
    def test_unmarshalling(self):
        for serializedval, valtype, nativeval in marshalled_value_pairs:
            unmarshaller = lookup_casstype(valtype)
            whatwegot = unmarshaller.from_binary(serializedval)
            self.assertEqual(whatwegot, nativeval,
                             msg='Unmarshaller for %s (%s) failed: unmarshal(%r) got %r instead of %r'
                                 % (valtype, unmarshaller, serializedval, whatwegot, nativeval))
            self.assertEqual(type(whatwegot), type(nativeval),
                             msg='Unmarshaller for %s (%s) gave wrong type (%s instead of %s)'
                                 % (valtype, unmarshaller, type(whatwegot), type(nativeval)))

    def test_marshalling(self):
        for serializedval, valtype, nativeval in marshalled_value_pairs:
            marshaller = lookup_casstype(valtype)
            whatwegot = marshaller.to_binary(nativeval)
            self.assertEqual(whatwegot, serializedval,
                             msg='Marshaller for %s (%s) failed: marshal(%r) got %r instead of %r'
                                 % (valtype, marshaller, nativeval, whatwegot, serializedval))
            self.assertEqual(type(whatwegot), type(serializedval),
                             msg='Marshaller for %s (%s) gave wrong type (%s instead of %s)'
                                 % (valtype, marshaller, type(whatwegot), type(serializedval)))
