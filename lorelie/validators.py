from typing import Any
from lorelie.exceptions import ValidationError


class BaseValidator:
    def __call__(self, value: Any):
        pass


class NumberCommaSeparatedValidator(BaseValidator):
    """A validator to ensure all items in a comma-separated 
    string are digits."""

    def __call__(self, value: Any):
        if not isinstance(value, str):
            raise ValidationError(f"Value '{value}' is not a string")

        items = value.split(',')
        for item in items:
            if not item.isdigit():
                raise ValidationError(f"Value '{item}' is not a digit")


number_comma_separated_validator = NumberCommaSeparatedValidator()


class StringCommaSeparatedValidator(BaseValidator):
    """A validator to ensure all items in a comma-separated 
    string are strings."""

    def __call__(self, value: Any):
        if not isinstance(value, str):
            raise ValidationError(f"Value '{value}' is not a string")

        items = value.split(',')
        for item in items:
            if not isinstance(item, str):
                raise ValidationError(f"Value '{item}' is not a string")


string_comma_separated_validator = StringCommaSeparatedValidator()


class RegexValidator:
    def __init__(self, regex=None):
        self.regex = regex

    def __call__(self, text):
        pass


class URLValidator(RegexValidator):
    def __call__(self, value):
        if not value.startswith('http'):
            raise ValidationError(f"Url is not valid {value}")


url_validator = URLValidator()


# class EmailValidator(RegexValidator):
#     def __call__(self, text):
#         pass


# email_validator = EmailValidator()


# class MinValueValidator(BaseValidator):
#     def __init__(self, limit):
#         self.limit = limit

#     def __call__(self, value):
#         if value < self.limit:
#             raise ValidationError(
#                 "Value '{value}' is over the limit of '{limit}'"
#                 "required by the {class_name}",
#                 value=value,
#                 limit=self.limit,
#                 class_name=self.__class__.__name__
#             )


# class MaxValueValidator(MinValueValidator):
#     def __call__(self, value):
#         if value > self.limit:
#             raise ValidationError(
#                 "Value '{value}' is under the limit of '{limit}'"
#                 "required by the {class_name}",
#                 value=value,
#                 limit=self.limit,
#                 class_name=self.__class__.__name__
#             )


# class FileExtensionValidator:
#     def __init__(self, accepted_extensions=[]):
#         pass

#     def __call__(self, text):
#         pass


# def image_extensions():
#     try:
#         from PIL import Image
#     except ImportError:
#         return []
#     else:
#         Image.init()
#         return [ext.lower()[1:] for ext in Image.EXTENSION]


# def image_extension_validator(value):
#     extensions = image_extensions()
#     validator = FileExtensionValidator(accepted_extensions=extensions)
#     validator(value)


# class MaxLengthValidator:
#     pass


# class MinLengthValidator:
#     pass
