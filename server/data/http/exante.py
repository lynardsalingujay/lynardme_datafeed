from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from urllib.parse import quote, unquote

from requests import get as http_get
from pandas import DataFrame, to_datetime

from utils.cloud_logging import logger

from utils import swallow_and_log_exception


class ExanteClient:

    @classmethod
    def create(cls, status: str):
        if status == "mock":
            return ExanteClientMock()
        elif status in ["live", "demo"]:
            return ExanteClientHTTP(status=status)
        else:
            raise TypeError('ExanteClient can have status in ["live", "demo", "mock"]')

    def __init__(self):
        self.pool = ThreadPoolExecutor()

    @staticmethod
    def remove_none_values(**params) -> dict:
        queryParams = {}
        for key, value in params.items():
                if value is not None:
                    queryParams[key] = value
        return queryParams

    @staticmethod
    def as_timestamp(x: [datetime, int]) -> int:
        if isinstance(x, datetime):
            return int(x.timestamp())
        elif isinstance(x, int):
            return x
        else:
            return x

    def OHLC(self, symbol: str, bar_length_seconds: int, number_of_bars: int = None,
             end_time: [datetime, int] = None, start_time: [datetime, int] = None):
        if (end_time is None and start_time is not None) or (end_time is not None and start_time is None):
            raise ValueError("start_time and end_time must either both be None or both be a time")
        queryParams = ExanteClient.remove_none_values(**{"size": number_of_bars,
                                                         "to": ExanteClient.as_timestamp(end_time),
                                                         "from": ExanteClient.as_timestamp(start_time)})
        data = self.api_request("ohlc", symbol, bar_length_seconds, **queryParams)
        if 'error' in data:
            df = DataFrame.from_records([data])
        else:
            df = DataFrame.from_records(data)
            df.loc[:, "datetime"] = to_datetime(df["timestamp"], unit="ms")
        return df

    def latest_price(self, symbol: str):
        bar = self.OHLC(symbol, bar_length_seconds=60, number_of_bars=1)
        bar.loc[:, 'symbol'] = symbol
        return bar[['symbol', 'datetime', 'close']].to_dict(orient='records')[0]

    def crossrate(self, currency_from: str, currency_to: str):
        data = self.api_request("crossrates", currency_from, currency_to)
        data['currency_from'] = currency_from
        data['currency_to'] = currency_to
        return data

    def gbp_rate(self, currency_from: str):
        return self.crossrate(currency_from, 'GBP')

    def api_request(self, endpoint, *uriParams, **queryParams):
        raise NotImplementedError('use a specific base class')

    INDEX_SUFFIX = ".INDEX"

    def nearest(self, symbol):
        return self.api_request("groups", symbol, "nearest")

    def batch_api_request(self, method, *parameters):
        return list(self.pool.map(method, *parameters))


class ExanteClientHTTP(ExanteClient):

    BASE_URL_API = "/md/1.0"

    LIVE_HOST = "https://api-live.exante.eu"

    DEMO_HOST = "https://api-demo.exante.eu"

    LIVE_AUTH = ("3e598756-7d1d-4d97-967e-4074994525ee", "90B5c00jhASsBrIvEExc")

    DEMO_AUTH = ("", "")

    def __init__(self, status: str):
        super().__init__()
        if status == "live":
            self.auth = self.LIVE_AUTH
            self.url_host = self.LIVE_HOST
        elif status == "demo":
            pass
        else:
            raise ValueError("status must be 'live' or 'demo'")

        self.base_url = self.url_host + self.BASE_URL_API

    def build_url(self, endpoint, *uriParams):
        url = self.base_url + "/" + quote(endpoint)
        for param in uriParams:
            url += "/" + unquote(str(param))
        return url

    def api_request(self, endpoint, *uriParams, **queryParams):
        url = self.build_url(endpoint, *uriParams)
        response = http_get(url, auth=self.auth, params=queryParams)
        if response.status_code == 200:
            return response.json()
        else:
            return {"error": str(response.content),
                    "status_code": response.status_code}


class ExanteClientMock(ExanteClient):

    ESU9_FUTURE_BARS_300 = DataFrame(data=[[datetime(2019, 8, 1, 14, 0).timestamp()*1000, 2000.0],
                                           [datetime(2019, 8, 1, 14, 5).timestamp()*1000, 2000.5],
                                           [datetime(2019, 8, 1, 14, 10).timestamp()*1000, 2001.0],
                                           [datetime(2019, 8, 1, 14, 15).timestamp()*1000, 2001.5],
                                           [datetime(2019, 8, 1, 14, 20).timestamp()*1000, 2002.0],
                                           [datetime(2019, 8, 1, 14, 25).timestamp()*1000, 2002.5],
                                           [datetime(2019, 8, 1, 14, 30).timestamp()*1000, 2003.0]],
                                     columns=["timestamp", "close"])

    SPX_INDEX_BARS_300 = DataFrame(data=[[datetime(2019, 8, 1, 14, 0).timestamp()*1000, 2000.1],
                                         [datetime(2019, 8, 1, 14, 5).timestamp()*1000, 2000.6],
                                         [datetime(2019, 8, 1, 14, 10).timestamp()*1000, 2001.1],
                                         [datetime(2019, 8, 1, 14, 15).timestamp()*1000, 2001.6],
                                         [datetime(2019, 8, 1, 14, 20).timestamp()*1000, 2002.1],
                                         [datetime(2019, 8, 1, 13, 50).timestamp()*1000, 1999.1],
                                         [datetime(2019, 8, 1, 13, 55).timestamp()*1000, 1999.6]],
                                   columns=["timestamp", "close"])

    ESU9_FUTURE_BARS_60 = DataFrame(data=[[datetime(2019, 8, 1, 16, 5).timestamp() * 1000, 2012.0]],
                                    columns=["timestamp", "close"])

    SPX_INDEX_BARS_60 = DataFrame(data=[[datetime(2019, 8, 1, 14, 20).timestamp() * 1000, 2002.1]],
                                  columns=["timestamp", "close"])

    MOCK_DATA = {("ohlc", "SPX.INDEX", 300): SPX_INDEX_BARS_300,
                 ("ohlc", "SPX.INDEX", 60): SPX_INDEX_BARS_60,
                 ("ohlc", "ES.CME.U2019", 300): ESU9_FUTURE_BARS_300,
                 ("ohlc", "ES.CME.U2019", 60): ESU9_FUTURE_BARS_60,
                 ("groups", "SPX", "nearest"): {"id": "ES.CME.U2019", 'mpi': 0.25}}

    def api_request(self, endpoint, *uriParams, **queryParams):
        key = [endpoint] + list(uriParams)
        key = tuple(key)
        return ExanteClientMock.MOCK_DATA[key]


if __name__ == "__main__":
    client = ExanteClient.create(status="live")
    x = client.nearest('SPX')
    print(x)