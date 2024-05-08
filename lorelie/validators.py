class BaseValidator:
    pass


class RegexValidator:
    pass


class URLValidator:
    pass


class EmailValidator:
    def __call__(self, text):
        pass


email_validator = EmailValidator()


class MaxValueValidator:
    pass


class MinValueValidator:
    pass


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
    return validator(value)


class MaxLengthValidator:
    pass


class MinLengthValidator:
    pass
