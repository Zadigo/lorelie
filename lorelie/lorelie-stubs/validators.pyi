from typing import Any


class BaseValidator:
    pass


class RegexValidator:
    pass


class URLValidator:
    pass


class EmailValidator:
    def __call__(self, text: Any) -> None: ...


email_validator: EmailValidator


class MaxValueValidator:
    pass


class MinValueValidator:
    pass


class FileExtensionValidator:
    def __init__(self, accepted_extensions: list = ...) -> None: ...
    def __call__(self, text) -> None: ...


def image_extensions() -> list[str]: ...


def image_extension_validator(value) -> None: ...


class MaxLengthValidator:
    pass


class MinLengthValidator:
    pass
