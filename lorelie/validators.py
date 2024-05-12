from lorelie.exceptions import ValidationError


class BaseValidator:
    def __call__(self, value):
        return value


class RegexValidator:
    def __init__(self, regex=None):
        self.regex = regex

    def __call__(self, text):
        pass


class URLValidator(RegexValidator):
    pass


url_validator = URLValidator()


class EmailValidator(RegexValidator):
    def __call__(self, text):
        pass


email_validator = EmailValidator()


class MinValueValidator(BaseValidator):
    def __init__(self, limit):
        self.limit = limit

    def __call__(self, value):
        if value < self.limit:
            raise ValidationError(
                "Value '{value}' is over the limit of '{limit}'"
                "required by the {class_name}",
                value=value,
                limit=self.limit,
                class_name=self.__class__.__name__
            )


class MaxValueValidator(MinValueValidator):
    def __call__(self, value):
        if value > self.limit:
            raise ValidationError(
                "Value '{value}' is under the limit of '{limit}'"
                "required by the {class_name}",
                value=value,
                limit=self.limit,
                class_name=self.__class__.__name__
            )


class FileExtensionValidator:
    def __init__(self, accepted_extensions=[]):
        pass

    def __call__(self, text):
        pass


def image_extensions():
    try:
        from PIL import Image
    except ImportError:
        return []
    else:
        Image.init()
        return [ext.lower()[1:] for ext in Image.EXTENSION]


def image_extension_validator(value):
    extensions = image_extensions()
    validator = FileExtensionValidator(accepted_extensions=extensions)
    validator(value)


class MaxLengthValidator:
    pass


class MinLengthValidator:
    pass
