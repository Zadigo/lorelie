from lorelie.database.functions.base import Functions


class ExtractDatePartsMixin(Functions):
    date_part: str = ...


class Extract(ExtractDatePartsMixin):
    def __init__(self, field_name: str, part: str) -> None: ...


class ExtractYear(ExtractDatePartsMixin):
    ...


class ExtractMonth(ExtractDatePartsMixin):
    ...


class ExtractDay(ExtractDatePartsMixin):
    ...


class ExtractHour(ExtractDatePartsMixin):
    ...


class ExtractMinute(ExtractDatePartsMixin):
    ...
