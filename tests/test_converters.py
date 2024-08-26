import datetime

import pytz

from lorelie.converters import (convert_boolean, convert_date,
                                convert_datetime, convert_timestamp)
from lorelie.test.testcases import LorelieTestCase


class TestConverters(LorelieTestCase):
    def test_convert_boolean(self):
        values = [
            ('0', False),
            ('1', True)
        ]
        for lhv, rhv in values:
            with self.subTest(value=lhv):
                lhv = lhv.encode('utf-8')
                self.assertEqual(convert_boolean(lhv), rhv)

    def test_convert_timestamp(self):
        value = datetime.datetime.now(tz=pytz.UTC)
        result = convert_timestamp(value.timestamp())
        self.assertEqual(value.date(), result.date())

    def test_convert_datetime(self):
        d = datetime.datetime.now(tz=pytz.UTC)
        result = convert_datetime(str(d).encode('utf-8'))
        self.assertIsInstance(result, datetime.datetime)

        d = datetime.datetime.now()
        result = convert_datetime(str(d).encode('utf-8'))
        self.assertIsInstance(result, datetime.datetime)
