from typing import override

from lorelie.database.functions.base import Functions


class Lower(Functions):
    ...


class Upper(Functions):
    ...


class Length(Functions):
    ...


class MD5Hash(Functions):
    ...


class SHA1Hash(MD5Hash):
    ...


class SHA224Hash(MD5Hash):
    ...


class SHA256Hash(MD5Hash):
    ...


class SHA384Hash(MD5Hash):
    ...


class SHA512Hash(MD5Hash):
    ...


class Trim(Functions):
    ...


class LTrim(Trim):
    ...


class RTrim(Trim):
    ...


class SubStr(Functions):
    @override
    def __init__(self, field_name: str, start: int, end: int) -> None: ...


class Concat(Functions):
    fields: list[str] = ...

    @override
    def __init__(self, *fields: list) -> None: ...
