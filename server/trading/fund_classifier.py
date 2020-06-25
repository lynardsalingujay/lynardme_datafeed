from pandas import DataFrame, concat, date_range, DatetimeIndex, to_datetime, Timedelta
import pandas_market_calendars as market_calendars
from pandas.tseries.offsets import CustomBusinessDay
from numpy import nan, arange
from bokeh.plotting import figure
from bokeh.transform import dodge
from bokeh.models import ColumnDataSource, NumeralTickFormatter, HoverTool
from app.models import Price, Universe
from django_pandas.io import read_frame
from app.cache import cache_result
from app.enums import one_day, five_minutes, close, mid, ask, bid, index_future, equity_index, trade

GEO_MAPPING = {
    'SPX Index': 'US',
    'TPX Index': 'JP'
}

reference_price_time_utc = {'TP1 Index': '06:00:00', 'NK1 Index': '06:00:00', 'ES1 Index': '20:00:00',
                        'HI1 Index': '07:10:00', 'GBPJPY Curncy': '12:00:00', 'GBPUSD Curncy': '12:00:00',
                        'GBPHKD Curncy': '12:00:00', 'TPX Index': '06:00:00', 'SPX Index': '20:00:00', 'HSI Index': '07:10:00'}

FUTURE_MAPPING = {
    'SPX Index': 'ES1 Index',
    'TPX Index': 'TP1 Index'
}

INDEX_TO_FUTURE_PREFIX = {'SPX Index': 'ES', 'TPX Index': 'TP'}

holiday_aliases = {'TP1 Index': 'JPX', 'NK1 Index': 'JPX', 'TPX Index': 'JPX', 'SPX Index': 'NYSE',
                   'ES1 Index': 'NYSE', 'HI1 Index': 'HKEX', 'HSI Index': 'HKEX'}


def get_time_slices(start_date, end_date, time_or_times, timezone='Europe/London'):
    dt_range = date_range(start_date, end_date, tz=timezone, freq='5min')
    df = dt_range.to_frame()
    if isinstance(time_or_times, str):
        idx = df.at_time(time_or_times).index.tz_convert('UTC')
    else:
        try:
            leng = len(time_or_times)
        except Exception:
            idx = df.at_time(time_or_times).index.tz_convert('UTC')
        else:
            if leng == 2:
                idx = df.between_time(time_or_times[0], time_or_times[1]).index.tz_convert('UTC')
            else:
                raise AttributeError('fx_time must be a datetime.time type or an iterable of start_time and end_time')

    idx = idx.tz_localize(None)
    datetime_list = idx.strftime('%Y-%m-%d %H:%M:%S').tolist()
    return datetime_list

def read_prices(**filter_criteria):
    query_set = Price.objects.filter(**filter_criteria)
    df = read_frame(query_set)
    return df

def read_universe(**filter_criteria):
    query_set = Universe.objects.filter(**filter_criteria)
    df = read_frame(query_set)
    return df

def remove_holidays(df, holidays=None):
    '''
    Remove weekend timestamps from a datetime index
    :param dt_index: Datetime Index
    :return: Datetime Index
    '''
    from numpy import isin
    result = df[df.index.dayofweek < 5]
    if holidays is not None:
        result = result[~isin(result.index.date.astype('datetime64[D]'), holidays)]
    return result


def get_fund_idx_fx_p_dic(price_dic, fund, index, fx):
    fx_prices = price_dic['bar_price'][(price_dic['bar_price']['symbol'].eq(fx))]
    pivot_p_dic = {'pivot_idx_fund_p': price_dic['daily_price'], 'pivot_fx_p': fx_prices}
    for name, price in pivot_p_dic.items():
        pivot_p_dic[name] = price.pivot_table(index='time', columns='symbol', values='value')
    for asset in [fund, index]:
        pivot_p_dic['pivot_idx_fund_p'][asset].index = pivot_p_dic['pivot_idx_fund_p'][asset].index.tz_localize(None)
    pivot_p_dic['pivot_fx_p'].name = fx

    data_dic = {'fx': pivot_p_dic['pivot_fx_p'], 'fund': pivot_p_dic['pivot_idx_fund_p'][fund],
                'index': pivot_p_dic['pivot_idx_fund_p'][index]}
    return data_dic

def get_formatted_daily_p_dic(fund_idx_fx_p, start_dt, end_dt, index_offset_days, include_holidays):
    sliced_fx_p = fund_idx_fx_p['fx'].copy()
    if sliced_fx_p.index.tz is None:
        sliced_fx_p.index = sliced_fx_p.index.tz_localize('UTC')
    sliced_fx_p.index = sliced_fx_p.index.tz_convert('Europe/London')
    sliced_fx_p.index = to_datetime(sliced_fx_p.index.date)
    sliced_fx_p = sliced_fx_p[start_dt:end_dt]

    dt_range = date_range(start=start_dt, end=end_dt)
    df = DataFrame(index=dt_range)

    fund_symbol = fund_idx_fx_p['fund'].name
    index_symbol = fund_idx_fx_p['index'].name
    fx_symbol = fund_idx_fx_p['fx'].name

    local_offset_index, df = get_offset_date_index(df, fund_idx_fx_p['index'].name,
                                                   index_offset_days, include_holidays)

    df.loc[:, fund_symbol] = fund_idx_fx_p['fund'].reindex(df.index)

    df.loc[:, index_symbol] = fund_idx_fx_p['index'].reindex(dt_range).fillna(
        method='ffill').reindex(local_offset_index).values
    df.loc[:, fx_symbol] = sliced_fx_p.reindex(df.index)
    result = df.iloc[1:, :].copy()
    return {'fx': result[fx_symbol], 'fund': result[fund_symbol], 'index': result[index_symbol]}


def get_offset_date_index(df, index, index_offset_days,  include_holidays):
    regular_holidays_local = market_calendars.get_calendar(holiday_aliases[index]).regular_holidays.holidays()
    regular_holidays_local_array = regular_holidays_local.values.astype('<M8[D]')
    regular_holidays_lse = market_calendars.get_calendar('LSE').regular_holidays.holidays()
    df = remove_holidays(df, holidays=regular_holidays_lse)
    if include_holidays:
        custom_bus_days = CustomBusinessDay(index_offset_days, holidays=regular_holidays_local_array)
    else:
        custom_bus_days = CustomBusinessDay(index_offset_days)
    local_date_index = to_datetime(df.index.date + custom_bus_days)
    return local_date_index, df


def get_spreads_by_date(bar_data_df: DataFrame, index: str):
    und_index = index
    future = FUTURE_MAPPING[index]
    spreads = bar_data_df[future] - bar_data_df[und_index]
    spreads.name = 'spreads'
    start_time = (to_datetime(reference_price_time_utc[future]) - Timedelta(minutes=50)).time()
    spread_df = spreads.between_time(start_time, reference_price_time_utc[future]).to_frame()
    spread_df['trading_date_utc'] = spread_df.index.date
    trading_day_index = DatetimeIndex(spread_df['trading_date_utc'])
    spread_df['25% quantile'] = spread_df.groupby('trading_date_utc').quantile(0.25).reindex(index=trading_day_index)[
        'spreads'].values
    spread_df['75% quantile'] = spread_df.groupby('trading_date_utc').quantile(0.75).reindex(index=trading_day_index)[
        'spreads'].values
    spread_df['eligible_spreads'] = (spread_df['spreads'] > spread_df['25% quantile']) & \
                                    (spread_df['spreads'] < spread_df['75% quantile'])
    spread_df['spreads'] = spread_df['spreads'].where(spread_df['eligible_spreads'], nan)
    eligible_spreads_by_date = spread_df.groupby('trading_date_utc').mean()['spreads']
    return eligible_spreads_by_date


def append_signals(bar_data_df: DataFrame, idx_daily_p: DataFrame, index, include_holidays, index_offset_days):
    local_date_index, df = get_offset_date_index(bar_data_df, index,
                                                 index_offset_days, include_holidays)
    # if the offset day is local holiday, then use the previous day's value
    index_ref_closing_price = idx_daily_p.reindex(index=local_date_index).fillna(method='ffill')
    df.loc[:, 'ref_index_date_local'] = local_date_index
    spreads = get_spreads_by_date(df, index).reindex(index=local_date_index).fillna(method='ffill')
    fut_contract = FUTURE_MAPPING[index]

    df.loc[:, 'spread_to_index'] = spreads.values
    df.loc[:, 'ref_index_price'] = (index_ref_closing_price + spreads).values
    df.loc[:, 'Signal at val time'] = df[fut_contract] / df['ref_index_price'] - 1
    return df


def get_returns(price_df_dic):
    result = {}
    for asset, price_series in price_df_dic.items():
        result[asset] = price_series.pct_change()
    return result


def get_residual_return(return_df_dic, fx_hedged):
    if fx_hedged:
        residual_return = return_df_dic['fund'] - return_df_dic['index']
    else:
        residual_return = return_df_dic['fund'] - return_df_dic['index'] + return_df_dic['fx']
    result = residual_return[1:]
    result.name = 'Residual return'
    return result


def get_correlation(return_df_dic, fx_hedged):
    if fx_hedged:
        hedging_return = return_df_dic['index']
    else:
        hedging_return = return_df_dic['index'] - return_df_dic['fx']

    return return_df_dic['fund'].corr(hedging_return)


def get_beta(return_df_dic, fx_hedged):
    if fx_hedged:
        hedging_return = return_df_dic['index']
    else:
        hedging_return = return_df_dic['index'] - return_df_dic['fx']

    covariance_fund_hedge = return_df_dic['fund'].cov(hedging_return)
    variance_hedge = hedging_return.var()
    beta = covariance_fund_hedge / variance_hedge
    return beta

def get_std_residuals(residual_return):
    return residual_return.std()


def plot_residual_returns_and_signal(return_df):
    return_df.index.name = 'Date'
    return_df = return_df.reset_index()
    return_df['Date'] = return_df['Date'].astype(str)
    maxi = max(return_df['Residual return'].max(), return_df['Signal at val time'].max())
    mini = min(return_df['Residual return'].min(), return_df['Signal at val time'].min())
    from math import ceil
    scale = ceil(max(maxi, -mini)*100)/100
    return_df = return_df.rename(columns={'Residual return': 'Residual', 'Signal at val time': 'Signal'})
    source = ColumnDataSource(return_df)
    fig = figure(x_range=return_df['Date'].to_list(), tools="pan,reset,save", sizing_mode='stretch_both')
    bar1 = fig.vbar(x=dodge('Date', -0.1, range=fig.x_range), top='Residual', width=0.2, source=source,
                    color='#4e73df', legend_label='Residual')
    bar2 = fig.vbar(x=dodge('Date', 0.1, range=fig.x_range), top='Signal', width=0.2, source=source,
                    color='red', legend_label='Signal')
    hover_residual = HoverTool(tooltips=[("Date", "@Date"), ("Residual", "@Residual{0.00%}")], renderers=[bar1])
    fig.add_tools(hover_residual)
    hover_signal = HoverTool(tooltips=[("Date", "@Date"), ("Signal", "@Signal{0.00%}")], renderers=[bar2])
    fig.add_tools(hover_signal)
    fig.yaxis.ticker = arange(-scale, scale, 0.001).tolist()
    fig.yaxis.formatter = NumeralTickFormatter(format='0.00%')
    fig.xaxis.major_label_orientation = "vertical"
    return fig


@cache_result(timeout_s=24*60*60)
def get_daily_prices(start_dt, end_dt, fund, index):
    daily_prices = read_prices(time__gte=start_dt, time__lte=end_dt, resolution=one_day, aspect=close,
                               symbol__in=[fund, index])
    return daily_prices


@cache_result(timeout_s=24*60*60)
def get_bar_prices(start_dt, end_dt, index, fx, fx_time):
    val_time_time_slice = get_time_slices(start_dt, end_dt, fx_time)
    fx_prices = read_prices(time__in=val_time_time_slice, resolution=five_minutes, aspect=close, symbol=fx,
                            price_type__in=[mid, trade])
    fx_prices = fx_prices.groupby(['symbol', 'time']).mean().reset_index()
    end_dt_timestamp = to_datetime(end_dt) + Timedelta(days=5)
    new_end_dt = end_dt_timestamp.strftime('%Y-%m-%d')
    future_symbol_prefix = INDEX_TO_FUTURE_PREFIX[index]
    end_time_spread = reference_price_time_utc[index]
    val_time_time_slice = get_time_slices(start_dt, new_end_dt, fx_time)
    start_time_spread = (to_datetime(end_time_spread) - Timedelta(minutes=60)).time().strftime('%H:%M:%S')
    spread_time_slice = get_time_slices(start_dt, new_end_dt, [start_time_spread, end_time_spread], timezone='UTC')
    fut_bar_time_slice = list(set(val_time_time_slice + spread_time_slice))
    fut_5m_p = read_prices(time__in=fut_bar_time_slice, resolution=five_minutes,
                           asset_type=index_future, symbol__startswith=future_symbol_prefix)
    idx_5m_p = read_prices(time__in=spread_time_slice, symbol=index, resolution=five_minutes)
    return concat([fx_prices, fut_5m_p, idx_5m_p], sort=True)

@cache_result(timeout_s=24*60*60)
def get_universe(index):
    future_prefix = INDEX_TO_FUTURE_PREFIX[index]
    df = read_universe(symbol__startswith=future_prefix)
    df = df.sort_values('as_of', ascending=False)
    df = df.drop_duplicates(subset='symbol', keep='first')
    return df.drop('id', axis='columns')

def get_prices(start_dt, end_dt, fund, index, fx, fx_time):
    daily_prices = get_daily_prices(start_dt, end_dt, fund, index)
    bar_prices = get_bar_prices(start_dt, end_dt, index, fx, fx_time)
    return {'daily_price': daily_prices, 'bar_price': bar_prices}


def get_structured_prices(price_dic, start_dt, end_dt, fund, index, fx, index_offset_days, include_holidays):
    fund_idx_fx_p = get_fund_idx_fx_p_dic(price_dic, fund, index, fx)
    daily_p = get_formatted_daily_p_dic(fund_idx_fx_p, start_dt, end_dt, index_offset_days, include_holidays)
    bar_p_raw = dict.fromkeys([index_future, equity_index])
    for asset in bar_p_raw:
        bar_p_raw[asset] = price_dic['bar_price'][price_dic['bar_price']['asset_type'].eq(asset)]

    bar_p_df = get_formatted_bar_p(bar_p_raw, start_dt, end_dt, index)
    return {'residual_return_data': daily_p, 'signal_data': (bar_p_df, fund_idx_fx_p['index'])}

def get_formatted_bar_p(bar_p, start_dt, end_dt, index):
    fut_bar = bar_p[index_future]
    idx_bar = bar_p[equity_index]
    list_of_df = []
    from trading.research.mft_backtest import get_stitched_future_series, get_avg_price_series
    universe = get_universe(index)
    future_price = get_stitched_future_series(fut_bar, start_dt, end_dt, index, universe, roll_days=1)
    future_price.name = FUTURE_MAPPING[index]
    future_price = future_price.to_frame()
    list_of_df.append(future_price)
    idx_price = get_avg_price_series(idx_bar, start_dt, end_dt, index)
    idx_price.name = index
    idx_price = idx_price.to_frame()
    list_of_df.append(idx_price)
    result = concat(list_of_df, axis='columns')
    return result

def get_signal(bar_p, daily_p, index, include_holidays, index_offset_days, val_time_london):
    idx_daily_p = remove_holidays(daily_p)
    df = append_signals(bar_p, idx_daily_p, index, include_holidays, index_offset_days)
    df.index = df.index.tz_convert('Europe/London')
    df = df.at_time(val_time_london)
    df.index = df.index.tz_localize(None)
    return df['Signal at val time']
#
def get_residual_return_and_signal_df(fund, index, fx, start_dt, end_dt,
                                   val_time_london, index_offset_days, fx_hedged, include_holidays):
    pass

def val_time_search_by_correlation(fund, index, fx, start_dt, end_dt):
    pass

def calculate_fund_classification(fund, index, fx, has_currency_hedge, index_offset, fx_time, start_date,
                                  end_date, include_holidays, **kwargs):

    price_dic = get_prices(start_date, end_date, fund, index, fx, fx_time)
    structured_p = get_structured_prices(price_dic, start_date, end_date, fund, index, fx, index_offset, include_holidays)
    structured_bar_p, daily_idx_p = structured_p['signal_data']
    return_df_dic = get_returns(structured_p['residual_return_data'])

    residual_return = get_residual_return(return_df_dic, has_currency_hedge)
    correlation = get_correlation(return_df_dic, has_currency_hedge)
    beta = get_beta(return_df_dic, has_currency_hedge)
    # std_residual = get_std_residuals(residual_return)
    signal = get_signal(structured_bar_p, daily_idx_p, index, include_holidays, index_offset, fx_time)
    signal.index = signal.index.date
    residual_return.index = residual_return.index.date
    signal_reindex = signal.reindex(index=residual_return.index)
    residual_and_signal_df = concat([residual_return, signal_reindex], axis='columns')
    bokeh_fig = plot_residual_returns_and_signal(residual_and_signal_df)

    return correlation, beta, bokeh_fig

