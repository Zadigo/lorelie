from lorelie.database.functions.base import Functions

class ExtractDatePartsMixin(Functions()):
    date_part: str = ...


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
