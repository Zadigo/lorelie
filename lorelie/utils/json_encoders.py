import dataclasses
import datetime
import decimal
import uuid
from json.encoder import JSONEncoder


class DefaultJSonEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            representation = obj.isoformat()
            if representation.endswith('+00:00'):
                representation = representation[:-6] + 'Z'
            return representation

        if isinstance(obj, datetime.time):
            if datetime.timezone and datetime.timezone.is_aware(obj):
                raise ValueError('Cannot represent timezone-aware times.')
            return obj.isoformat()

        if isinstance(obj, datetime.date):
            return str(obj)

        if isinstance(obj, datetime.timedelta):
            return str(obj.total_seconds())

        if isinstance(obj, decimal.Decimal):
            return float(obj)

        if dataclasses.is_dataclass(obj):
            return dict(obj)

        if isinstance(obj, uuid.UUID):
            return str(obj)

        if isinstance(obj, bytes):
            return obj.decode()

        return super().default(o)
