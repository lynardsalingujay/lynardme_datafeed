import string
from datetime import datetime
from random import choice, choices

from django.http import HttpResponse, JsonResponse
from pandas import DataFrame

from utils.cloud_logging import logger


def swallow_and_log_exception(default=None):
    def wrapper(function):
        def modified_function(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except:
                logger.exception('swallow_and_log_exception | fn:' + str(function)
                                 + ', args:' + str(args)
                                 + ', kwargs: ' + str(kwargs))
                return default
        return modified_function
    return wrapper


def random_int(max_size=100):
    return choice(range(0, max_size))


def random_string(max_length=10):
    n = choice(range(0, max_length))
    return ''.join(choices(string.ascii_uppercase + string.digits, k=n))


def random_strings(max_count=10, max_length=10):
    n = choice(range(0, max_count))
    strings = []
    for i in range(0, n):
        strings.append(random_string(max_length))
    return strings


def random_percentage():
    n = random_int(100)
    return n / 100


def percentage_formatter(number):
    return "{0:.2f}%".format(number * 100)


def float_formatter(number):
    return "{0:,.2f}".format(number)


def datetime_formatter(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')


def random_datetime():
    year = choice(range(2000, 2020))
    month = choice(range(1, 12))
    day = choice(range(1, 28))
    hour = choice(range(0, 23))
    minute = choice(range(0, 60))
    second = choice(range(0, 60))
    return datetime(year, month, day, hour, minute, second)


formatter_random_map = {None: random_string,
                        float_formatter: random_int,
                        percentage_formatter: random_percentage,
                        datetime_formatter: random_datetime}


def random_dataframe(columns=None, formatters=None):
    if columns is None:
        columns = random_strings()
    data = DataFrame(columns=columns)
    rows = range(0, 9)
    for col in columns:
        if formatters is None:
            formatter = None
        else:
            formatter = formatters.get(col, None)
        random_function = formatter_random_map.get(formatter, random_string)
        for row in rows:
            data.loc[row, col] = random_function()
    return data


def random_model(model_cls):
    from django.db.models.fields import DateTimeField, FloatField, CharField, TextField
    fields = model_cls._meta.get_fields()
    columns = []
    formatters = dict()
    for field in fields:
        columns.append(field.name)
        if isinstance(field, DateTimeField):
            formatters[field.name] = datetime_formatter
        elif isinstance(field, FloatField):
            formatters[field.name] = float_formatter
    return random_dataframe(columns, formatters)


def api_response(structure=None, formatters=None):

    def wrapper(function):

        def modified_function(request, status, response_type, *args, **kwargs):
            from django.db.models.base import ModelBase
            if status == 'mock':
                if request.method == 'POST':
                    return Message('POST request processed', 'success')
                elif structure is None:
                    data = random_dataframe()
                elif isinstance(structure, ModelBase):
                    data = random_model(structure)
                else:
                    data = random_dataframe(columns=structure, formatters=formatters)
                return as_response(data, response_type, formatters=formatters)
            elif status == 'live':
                data = function(request, *args, **kwargs)
                if structure is not None and not isinstance(structure, ModelBase):
                    columns = structure
                else:
                    columns = None
                return as_response(data, response_type, columns, formatters)
            else:
                return HttpResponse('status must be "mock" or "live"')

        return modified_function

    return wrapper


def apply_formatters(df: DataFrame, formatters):
    for col, f in formatters.items():
        df.loc[:, col] = df[col].apply(f)
    return df


def as_response(data: DataFrame, response_type: str, columns=None, formatters=None, **kwargs) -> HttpResponse:
    if columns is not None:
        data = data[columns]
    if response_type == "json":
        if formatters is not None:
            data = apply_formatters(data, formatters)
        return HttpResponse(data.to_json(orient="records", date_format="iso"))
    elif response_type == "html":
        return HttpResponse(data.to_html(columns=columns, formatters=formatters, **kwargs))


class Message:
    text: str
    status: str

    def __init__(self, text, status):
        self.text = text
        self.status = status

    def to_json(self, orient=None, date_format=None):
        return JsonResponse(self.__dict__)

    def to_html(self, columns=None, formatters=None, **kwargs):
        return HttpResponse(self.__dict__)


if __name__ == '__main__':
    from utils.test import CapturingStdOut
    from utils.cloud_logging import Logger
    logger = Logger('root', False, True)
    #with CapturingStdOut() as capture:
    print('this is a stdout')
    logger.debug('this is a debug')
    logger.info('this is an info')
    logger.warning('this is a warning')
    logger.error('this is an error')
    #capture.stderr_capture

    #import logging
    #logger = logging.getLogger('root')
    #handler = logging.StreamHandler()
    #logger.addHandler(handler)
    #logging.basicConfig(level=)
    #logger.setLevel(logging.DEBUG)
    #logger.debug('this is a debug')
    #logger.warning('this is a warning')
