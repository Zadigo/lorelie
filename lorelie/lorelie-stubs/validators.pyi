from typing import Any, override


class BaseValidator:
    def __call__(self, value: Any) -> None: ...


class RegexValidator:
    def __init__(self, regex: str = ...) -> None: ...


class URLValidator(RegexValidator):
    pass


url_validator: URLValidator


class EmailValidator:
    @override
    def __call__(self, text: str) -> None: ...


email_validator: EmailValidator


class MinValueValidator(BaseValidator):
    limit: int = ...

    @override
    def __init__(self, limit: int) -> None: ...
    @override
    def __call__(self, value: Any) -> None: ...


class MaxValueValidator(MinValueValidator):
    pass


class FileExtensionValidator:
    def __init__(self, accepted_extensions: list[str] = ...) -> None: ...
    def __call__(self, text) -> None: ...


def image_extensions() -> list[str]: ...


def image_extension_validator(value: Any) -> None: ...


class MaxLengthValidator:
    pass


class MinLengthValidator:
    pass
