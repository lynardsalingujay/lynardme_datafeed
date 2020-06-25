from pandas import Series, to_timedelta, to_datetime, DatetimeIndex, DataFrame, date_range, concat, Timedelta, isnull, NaT
from pandas.tseries.offsets import BDay, CustomBusinessDay
import pandas_market_calendars as market_calendars
from numpy import nan, busday_count, concatenate, datetime64
from app.enums import index_future, one_day, close, trade, nav, equity_index, fx_spot, open, high, low
from django.db.models import Q
from functools import reduce
from operator import or_
from django_pandas.io import read_frame
from app.models import Price, Universe, SimResult
from trading.fund_classifier import read_prices
from app.cache import cache_result

# ============================================ Default Configs ======================================================

# Local London time is 'Europe/London' this has DST
holiday_aliases = {'TP1 Index': 'JPX', 'NK1 Index': 'JPX', 'TPX Index': 'JPX', 'SPX Index': 'NYSE',
                   'ES1 Index': 'NYSE', 'HI1 Index': 'HKEX', 'HSI Index': 'HKEX'}

underlying_index = {'TP1 Index': 'TPX Index', 'NK1 Index': 'NKY Index',
                    'ES1 Index': 'SPX Index', 'HI1 Index': 'HSI Index'}

trading_hours_utc = {'TP1 Index': '23:45-20:30', 'NK1 Index': '23:45-20:30',
                     'ES1 Index': '21:00-20:00', 'HI1 Index': '01:15-19:00'}

# utc time for futures, london time for currencies
reference_price_time_utc = {'TP1 Index': '06:00:00', 'NK1 Index': '06:00:00', 'ES1 Index': '20:00:00',
                        'HI1 Index': '07:10:00', 'GBPJPY Curncy': '12:00:00', 'GBPUSD Curncy': '12:00:00',
                        'GBPHKD Curncy': '12:00:00', 'TPX Index': '06:00:00', 'SPX Index': '20:55:00', 'HSI Index': '07:10:00'}

# ======= data timezone from DB =====================
reference_price_timezone = {'TP1 Index': 'UTC', 'NK1 Index': 'UTC', 'ES1 Index': 'UTC', 'TPX Index': 'UTC',
                            'HI1 Index': 'UTC', 'GBPJPY Curncy': 'UTC', 'SPX Index': 'UTC',
                            'GBPUSD Curncy': 'UTC', 'GBPHKD Curncy': 'UTC', 'HSI Index': 'UTC'}

asset_local_timezone = {'TP1 Index': 'Asia/Tokyo', 'NK1 Index': 'Asia/Tokyo',
                        'ES1 Index': 'America/New_York', 'HI1 Index': 'Asia/Shanghai', 'TPX Index': 'Asia/Tokyo',
                        'SPX Index': 'America/New_York', 'HSI Index': 'Asia/Shanghai'}

day_offset = {'TP1 Index': 0, 'NK1 Index': 0, 'ES1 Index': -1, 'HI1 Index': 0,
              'GBPJPY Curncy': 0, 'GBPUSD Curncy': 0, 'GBPHKD Curncy': 0, 'TPX Index': 0, 'SPX Index': -1,
              'HSI Index': 0}

regular_holidays_lse = market_calendars.get_calendar('LSE').regular_holidays.holidays()
lse_holidays_array = regular_holidays_lse.values.astype('<M8[D]')
INDEX_TO_FUTURE_PREFIX = {'SPX Index': 'ES', 'TPX Index': 'TP'}
INDEX_TO_VOL_MAP = {'SPX Index': 'VIX Index', 'TPX Index': 'VNKY Index'}

def convert_index_tz_london(dt_index_tz):
    '''
    :param dt_index_tz: Timezone aware pandas datetime index
    :return: London local time pandas datetime index
    '''
    return dt_index_tz.tz_convert('Europe/London')


def remove_weekends(df):
    '''
    Remove weekend timestamps from a datetime index
    :param dt_index: Datetime Index
    :return: Datetime Index
    '''
    return df[df.index.dayofweek < 5]

def clean_daily_data(df):
    df = remove_weekends(df)
    return df# all original data are in asset's local timezone


# default configs
# settings

ltv = 0.75 # 4 times leverage; initial ltv 0.7
annual_interest_fund = 0.025
min_hold_period_days = 10
starting_capital_gbp = 5000000 # starting cash
variance_margin = 0.05
notional_per_trade_gbp = (starting_capital_gbp / (1 - ltv)) * (1 - variance_margin)  # maximum loan
loan_gbp = notional_per_trade_gbp - starting_capital_gbp

# Cash at start
# Cash at end
# Notional trade depend on cash position
# variation margin 5%
# interest on loan used

fund_dealing_charge_percent = 0.0002  # notional based
fx_spread_cost_as_percent = 0.00025
future_spread_cost_as_percent = {'TP1 Index': 0.0004, 'ES1 Index': 0.0001}


# exante or interactive shares costs
# 20% margin set aside for stocks

geo_setting = {'JP': 'TP1 Index', 'US': 'ES1 Index'}
geo_fx_map = {'JP': 'GBPJPY Curncy', 'US': 'GBPUSD Curncy'}
geo_vol_map = {'JP': 'VNKY Index', 'US': 'VIX Index'}

proportional_weights = 'TODO'

def get_stitched_future_series(fut_bar_price, start_dt, end_dt, underlying_index, universe, roll_days=2):
    """
    :param universe: DataFrame
    :param fut_bar_price: 5m bar prices for future contracts, DataFrame
    :param start_dt: String or Datetime
    :param end_dt: String or Datetime
    :param underlying_index: String
    :return: Series
    """
    fut_contracts = universe[universe['asset_type'].eq(index_future) & universe['expiry_date'].notnull()]
    underlying_index_map = fut_contracts.set_index('symbol')['underlying_index']
    fut_bar_price['underlying_index'] = fut_bar_price['symbol'].map(underlying_index_map)
    fut_universe = fut_contracts[fut_contracts['underlying_index'].eq(underlying_index)]
    fut_universe.loc[:, 'expiry_date'] = to_datetime(fut_universe['expiry_date'], format='%d/%m/%Y')
    end_dt_timestamp = to_datetime(end_dt) + Timedelta(days=92)
    end_dt = end_dt_timestamp.strftime('%Y-%m-%d')
    data_dt_range_idx = date_range(start=start_dt, end=end_dt, freq='5min', tz='UTC')

    regular_holidays_local = market_calendars.get_calendar(
        holiday_aliases[underlying_index]).regular_holidays.holidays()
    regular_holidays_local_array = regular_holidays_local.values.astype('<M8[D]')
    fut_ex_dt = fut_universe.set_index('expiry_date')['symbol']
    n_days_before_expiry_index = to_datetime(fut_ex_dt.index.date - CustomBusinessDay(roll_days, holidays=regular_holidays_local_array))

    fut_ex_dt.index = n_days_before_expiry_index
    fut_ex_dt.index = fut_ex_dt.index.tz_localize(tz='UTC')
    mapped_futures = fut_ex_dt.reindex(data_dt_range_idx).fillna(method='bfill').to_frame().dropna(how='all', axis='index').sort_index()

    aspects = ['open', 'high', 'low', 'close']
    stitched_price_dic = {}
    for aspect in aspects:
        p_df = (fut_bar_price[fut_bar_price['aspect'].eq(aspect) & fut_bar_price['underlying_index'].eq(underlying_index)])[['time', 'value', 'symbol']]
        p_df = p_df.pivot_table(index='time', columns='symbol', values='value').sort_index()
        # if p_df.index.tz is None:
        #     p_df.index = p_df.index.tz_localize('UTC')
        p_df.index = p_df.index.tz_convert(tz='UTC')
        p_df = p_df.reindex(mapped_futures.index)
        p_df['mapped_contract'] = mapped_futures['symbol']
        stitched_price_dic[aspect] = get_masked_values(p_df, 'mapped_contract')

    avg_price = (stitched_price_dic['open'] + stitched_price_dic['high'] + stitched_price_dic['low'] +
                       stitched_price_dic['close']) / 4

    return avg_price

def get_avg_price_series(bar_price_5m, start_dt, end_dt, asset_symbol, trade_p=True):
    data_dt_range_idx = date_range(start=start_dt, end=end_dt, freq='5min', tz='UTC')

    bar_price_5m = bar_price_5m[bar_price_5m['symbol'].eq(asset_symbol)]
    aspect_price_dic = {}
    if trade_p is False:
        quotes = ['bid', 'ask']
    else:
        quotes = ['trade']

    for quote in quotes:
        quote_price = bar_price_5m[bar_price_5m['price_type'].eq(quote)]
        quote_price = quote_price.pivot_table(index='time', columns='aspect', values='value').mean(axis='columns')
        quote_price.index = quote_price.index.tz_convert('UTC')
        quote_price = quote_price.reindex(index=data_dt_range_idx)
        aspect_price_dic[quote] = quote_price

    no_of_p_type = len(quotes)
    if no_of_p_type == 1:
        return aspect_price_dic['trade']
    else:
        return (aspect_price_dic['bid'] + aspect_price_dic['ask']) / no_of_p_type


def get_bar_price_df(dic_data, symbol_dic, universe_df, start_dt, end_dt, **configs):
    lst_of_df = []
    lst_of_idx = symbol_dic['index']
    lst_of_fut = symbol_dic['future']
    lst_of_fx = symbol_dic['fx']
    for idx, fut in zip(lst_of_idx, lst_of_fut):
        future_price = get_stitched_future_series(dic_data['fut_bar_p'], start_dt, end_dt, idx, universe_df)
        future_price.name = fut
        future_price = future_price.to_frame()
        lst_of_df.append(future_price)
        idx_price = get_avg_price_series(dic_data['idx_bar_p'], start_dt, end_dt, idx)
        idx_price.name = idx
        idx_price = idx_price.to_frame()
        lst_of_df.append(idx_price)
    for fx in lst_of_fx:
        fx_price = get_avg_price_series(dic_data['fx_bar_p'], start_dt, end_dt, fx, trade_p=False).fillna(method='ffill')
        fx_price.name = fx
        fx_price = fx_price.to_frame()
        lst_of_df.append(fx_price)
    result = concat(lst_of_df, axis='columns')
    result.index = result.index.tz_convert(tz='Europe/London')
    result.index = result.index.tz_localize(None)
    return result


def consolidate_bar_data(df, tz_of_data: str = 'Europe/London'):
    dfs_dic = {}
    for asset in df:
        dfs_dic[asset] = DataFrame(index=df.index, columns=['avg_trade'])
        # Getting the average trade price of OHLC
        dfs_dic[asset].loc[:, 'avg_trade'] = df[asset].copy()
        # Setting the timezone to London time for easier conversion later
        dfs_dic[asset].index = dfs_dic[asset].index.tz_localize(tz=tz_of_data, ambiguous='NaT')
        # Getting rid of the NaT timestamps resulted from the ambiguous time (DST related)
        dfs_dic[asset] = dfs_dic[asset][dfs_dic[asset].index.notnull()]
        dfs_dic[asset] = dfs_dic[asset]['avg_trade']
        # converting local timezone to relative timezone
        dfs_dic[asset].index = dfs_dic[asset].index.tz_convert(tz=reference_price_timezone[asset])
        # Getting rid of weekends
        dfs_dic[asset] = remove_weekends(dfs_dic[asset])

        reference_price_index = to_datetime(dfs_dic[asset].index.date
                                            ) + BDay(day_offset[asset]
                                                     ) + to_timedelta(reference_price_time_utc[asset])
        try:
            regular_holidays_local = market_calendars.get_calendar(
                holiday_aliases[asset]).regular_holidays.holidays()
            regular_holidays_local_array = regular_holidays_local.values.astype('<M8[D]')
        except KeyError:
            pass

        equity_trade_date_index = to_datetime(dfs_dic[asset].index.date + CustomBusinessDay(
            day_offset[asset] + 1, holidays=regular_holidays_local_array))

        df_result = dfs_dic[asset].to_frame()
        if 'Curncy' in asset:
            # Currency pricing time is 12:00:00 london time (Noon)
            reference_price_index = reference_price_index.tz_localize(tz='Europe/London')
            df_result['val_fx_timestamp'] = DatetimeIndex(reference_price_index)
            df_result[asset + ' val_price'] = dfs_dic[asset].reindex(reference_price_index).values
            equity_trade_datetime = (equity_trade_date_index + to_timedelta(reference_price_time_utc[asset])).tz_localize(tz='Europe/London')
            df_result['equity_trade_timestamp'] = DatetimeIndex(equity_trade_datetime)
            df_result[asset + ' equity_price'] = dfs_dic[asset].reindex(equity_trade_datetime).values
        else:
            reference_price_index = reference_price_index.tz_localize(tz=reference_price_timezone[asset])
            temp_masked_ref = Series(reference_price_index.where(
                reference_price_index <= dfs_dic[asset].index)).fillna(method='ffill')
            df_result['ref_future_timestamp'] = DatetimeIndex(temp_masked_ref)
            df_result['ref_future_price'] = dfs_dic[asset].reindex(reference_price_index).values

        try:
            local_holiday_bool = DatetimeIndex(reference_price_index.date).isin(regular_holidays_local)
        except KeyError:
            pass

        # add two boolean columns which indicate whether they are UK or local trading holidays
        uk_holiday_bool = DatetimeIndex(dfs_dic[asset].index.date).isin(regular_holidays_lse)
        df_result.loc[:, 'lse_holiday'] = uk_holiday_bool

        if 'Index' in asset:
            df_result.loc[:, 'local_exchange_holiday'] = local_holiday_bool
            df_result.loc[:, 'equity_trading_date'] = equity_trade_date_index
        dfs_dic[asset] = df_result

    return dfs_dic


def get_spreads_by_date(dic_of_bar_data: dict, future: str):
    und_index = underlying_index[future]
    spreads = dic_of_bar_data[future]['avg_trade'] - dic_of_bar_data[und_index].reindex(dic_of_bar_data[future].index)[
        'avg_trade']
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


def add_index_closing_price_based_signals(dic_of_bar_data: dict, daily_price_df: DataFrame,
                                          geo_settings: dict, **configs):
    for geo, fut_contract in geo_settings.items():
        vol_index = geo_vol_map[geo]
        local_date_index = DatetimeIndex(dic_of_bar_data[fut_contract]['ref_future_timestamp']
                                         ).tz_convert(tz=asset_local_timezone[fut_contract]).date
        index_ref_closing_price = daily_price_df[underlying_index[fut_contract]].reindex(index=local_date_index)
        dic_of_bar_data[fut_contract].loc[:, 'ref_index_date_local'] = local_date_index

        spreads = get_spreads_by_date(dic_of_bar_data, fut_contract).reindex(index=local_date_index)

        dic_of_bar_data[fut_contract].loc[:, 'spread_to_index'] = spreads.values
        dic_of_bar_data[fut_contract].loc[:, 'ref_index_price'] = (index_ref_closing_price + spreads).values
        dic_of_bar_data[fut_contract].loc[:, 'signal_based_on_ref_index'] = dic_of_bar_data[fut_contract]['avg_trade'
                                                                            ] / dic_of_bar_data[fut_contract][
                                                                                'ref_index_price'] - 1
        dic_of_bar_data[fut_contract].loc[:, 'volatility_index'] = daily_price_df[vol_index].reindex(index=local_date_index).fillna(method='ffill').values


def get_restructured_data(bar_prices, daily_prices, **kwargs):
    data_dic_by_asset = consolidate_bar_data(bar_prices)
    daily_price_df = clean_daily_data(daily_prices)
    # Querying from Django preserve tz information
    daily_price_df.index = daily_price_df.index.tz_localize(None)
    add_index_closing_price_based_signals(data_dic_by_asset, daily_price_df, **kwargs)
    return data_dic_by_asset, daily_price_df

def add_trading_daytime_cols(dic_of_bar_data, symbol_dic, **kwargs):
    list_of_futures = symbol_dic['future']
    for fut_contract in list_of_futures:
        index_london_time = dic_of_bar_data[fut_contract].index.tz_convert('Europe/London')
        dic_of_bar_data[fut_contract].loc[:, 'trading_day_utc'] = to_datetime(index_london_time.date)

        dic_of_bar_data[fut_contract].loc[:, 'trading_time_london'] = index_london_time.time
        eligible_trading_day = ~dic_of_bar_data[fut_contract]['local_exchange_holiday'
        ] & ~dic_of_bar_data[fut_contract]['lse_holiday']
        dic_of_bar_data[fut_contract].loc[:, 'eligible_trading_day'] = eligible_trading_day

        # Only apply to offset_t = 0
        eligible_trading_hours = (dic_of_bar_data[fut_contract]['trading_time_london'] >=
                                  kwargs['earliest_trading_time_london'][fut_contract]) & (
                                             dic_of_bar_data[fut_contract]['trading_time_london'] <
                                             kwargs['dealing_cutoff_time_london'])
        dic_of_bar_data[fut_contract].loc[:, 'eligible_trading_hours'] = eligible_trading_hours & eligible_trading_day


def create_backtest_df(processed_data_dic: dict, start_dt: str, end_dt: str, geo_settings: dict, **kwargs):
    backtest_range = date_range(start=start_dt, end=end_dt, freq='5min', tz='UTC')
    df = DataFrame(index=backtest_range)
    df = remove_weekends(df)
    for geo, fut in geo_settings.items():
        df.loc[:, geo + '_signal'] = processed_data_dic[fut]['signal_based_on_ref_index'].reindex(index=df.index)
        df.loc[:, geo + '_eligible_trading_time'] = processed_data_dic[fut]['eligible_trading_hours'].reindex(
            index=df.index)
        df.loc[:, fut] = processed_data_dic[fut]['avg_trade'].reindex(index=df.index)
        df.loc[:, fut + '_ref_time_price'] = processed_data_dic[fut]['ref_index_price'].reindex(index=df.index)
        df.loc[:, fut + '_spread'] = processed_data_dic[fut]['spread_to_index'].reindex(index=df.index)
        df.loc[:, geo + '_ref_index_date_local'] = processed_data_dic[fut]['ref_index_date_local'].reindex(
            index=df.index)
        df.loc[:, geo_fx_map[geo]] = processed_data_dic[geo_fx_map[geo]]['avg_trade'].reindex(index=df.index)
        df.loc[:, geo + '_volatility_index'] = processed_data_dic[fut]['volatility_index'].reindex(index=df.index)
        df.loc[:, fut + '_equity_trading_date_local'] = processed_data_dic[fut]['equity_trading_date'].reindex(index=df.index)
        # ====================== This is for getting valuation time fx rate, ========================================
        # ====================== since valuation time is based on the fund chosen, ==================================
        # ====================== it should be in another function in txs or position cal ============================
        fx_ref_price_name = geo_fx_map[geo] + ' val_price'
        df.loc[:, fx_ref_price_name] = processed_data_dic[geo_fx_map[geo]][fx_ref_price_name].reindex(index=df.index)
        df.loc[:, geo_fx_map[geo] + ' equity_price'] = processed_data_dic[geo_fx_map[geo]][geo_fx_map[geo] + ' equity_price'].reindex(index=df.index)
    return df


def get_trading_time_backtest_df(bktest_df: DataFrame, start_time_utc: str = '05:00:00',
                                 end_time_utc: str = '13:00:00'):
    result_df = bktest_df.between_time(start_time_utc, end_time_utc).copy()
    result_df.loc[:, 'trading_date_utc'] = to_datetime(result_df.index.date)
    return result_df


def append_txs_signals(trading_bktest_df: DataFrame, geo_settings: dict, **kwargs):
    vol_thresholds = kwargs.get('vol_based_thresholds')
    constraint = kwargs.get('fair_value_threshold_constraint', 100)

    if vol_thresholds is False:
        opn_threshold = kwargs.get('opening_signal_threshold')
        close_threshold = kwargs.get('closing_signal_threshold')
    else:
        opn_t_beta = kwargs.get('open_threshold_factors')[0]
        close_t_beta = kwargs.get('close_threshold_factors')[0]
        opn_t_constant = kwargs.get('open_threshold_factors')[1]
        close_t_constant = kwargs.get('close_threshold_factors')[1]

    for geo, fut in geo_settings.items():

        if vol_thresholds is True:
            opn_threshold = opn_t_beta * trading_bktest_df[geo + '_volatility_index'] + opn_t_constant
            close_threshold = close_t_beta * trading_bktest_df[geo + '_volatility_index'] + close_t_constant

        trading_bktest_df.loc[:, geo + '_open_tresholds'] = opn_threshold
        trading_bktest_df.loc[:, geo + '_close_thresholds'] = close_threshold

        working_open_txs_signals = (trading_bktest_df[geo + '_signal'].gt(opn_threshold) &
                                    trading_bktest_df[geo + '_signal'].lt(constraint) &
                                    trading_bktest_df[geo + '_eligible_trading_time']).astype(int)
        working_close_txs_signals = (trading_bktest_df[geo + '_signal'].lt(close_threshold) &
                                     trading_bktest_df[geo + '_signal'].gt(-constraint) &
                                     trading_bktest_df[geo + '_eligible_trading_time']).astype(int)
        trading_bktest_df.loc[:, geo + '_open_txs_signal'] = working_open_txs_signals.mask(working_open_txs_signals ==
                                                                                           working_open_txs_signals.shift(
                                                                                               1), 0)
        trading_bktest_df.loc[:, geo + '_close_txs_signal'] = working_close_txs_signals.mask(
            working_close_txs_signals ==
            working_close_txs_signals.shift(1), 0)
        # Getting rid of opening signals that ocurr multiple times in the same day, we just pick the first occured signal
        # One opening signal per geo per day
        daily_consecutive_signals_mask = trading_bktest_df.groupby('trading_date_utc').cumsum()[
            geo + '_open_txs_signal']
        daily_consecutive_signals_mask_close = trading_bktest_df.groupby('trading_date_utc').cumsum()[
            geo + '_close_txs_signal']
        trading_bktest_df.loc[:, geo + '_open_txs_signal'] = trading_bktest_df.loc[:, geo + '_open_txs_signal'].mask(
            daily_consecutive_signals_mask !=
            trading_bktest_df.loc[:, geo + '_open_txs_signal'], 0)
        trading_bktest_df.loc[:, geo + '_close_txs_signal'] = trading_bktest_df.loc[:, geo + '_close_txs_signal'].mask(
            daily_consecutive_signals_mask_close !=
            trading_bktest_df.loc[:, geo + '_close_txs_signal'], 0)


def get_opn_trade_indicator(bktest_df: DataFrame, geos: dict):
    # Comparing open signal size on each day and generate the larger one
    # Currently does the same for closing signals, but this is in fact not true. Closing signal on each day partially depends on the exisiting positions
    list_of_dfs = []
    signal_col_names = []
    opn_txs_signal_col_names = []
    working_txs_signal = []
    working_signal_suffix = '_open_trade'
    for geo, fut in geos.items():
        df = bktest_df[bktest_df[geo + '_eligible_trading_time'].eq(True)].copy()
        list_of_dfs.append(df)
        geo_signal = geo + '_signal'
        signal_col_names.append(geo_signal)
        geo_opn_txs_signal = geo + '_open_txs_signal'
        opn_txs_signal_col_names.append(geo_opn_txs_signal)
        geo_working_txs_signal = geo + working_signal_suffix
        working_txs_signal.append(geo_working_txs_signal)
    temp_concat_df = concat(list_of_dfs)
    for geo, fut in geos.items():
        temp_concat_df.loc[:, geo + working_signal_suffix
        ] = temp_concat_df[geo + '_signal'] * temp_concat_df[geo + '_open_txs_signal']
    temp_concat_df = temp_concat_df.drop_duplicates()
    temp_concat_df = temp_concat_df.sort_index()
    trade_choice = temp_concat_df[working_txs_signal].idxmax(axis='columns')
    trade_mask = temp_concat_df[opn_txs_signal_col_names].sum(axis='columns') > 0
    temp_concat_df.loc[:, 'geo_trade_choice'] = trade_choice[trade_mask]
    return temp_concat_df


def get_masked_values(bktest_df: DataFrame, col_name_to_mask: str, equity: bool=False):
    list_of_keys_to_mask = list(bktest_df[col_name_to_mask].dropna().unique())
    mask = DataFrame([list_of_keys_to_mask], index=[bktest_df.index[0]], columns=list_of_keys_to_mask)
    mask = mask.reindex(index=bktest_df.index).fillna(method='ffill')
    mask_2 = DataFrame(index=bktest_df.index, columns=list_of_keys_to_mask)
    mask_2.iloc[:, 0] = bktest_df[col_name_to_mask]
    mask_2 = mask_2.fillna(method='ffill', axis='columns')
    final_mask = mask.eq(mask_2)
    if equity is True:
        return bktest_df[list_of_keys_to_mask].where(final_mask, to_timedelta(0)).sum(axis='columns').replace(to_timedelta(0), nan)
    else:
        return bktest_df[list_of_keys_to_mask].where(final_mask, 0).sum(axis='columns').replace(0, nan)


def append_opn_trade_indicators(bktest_df, universe, geo_settings, **kwargs):
    universe = universe.set_index('symbol')
    df = get_opn_trade_indicator(bktest_df, geo_settings)
    bktest_df.loc[:, 'geo_trade_choice'] = df['geo_trade_choice'].reindex(index=bktest_df.index)
    bktest_df.loc[:, 'geo_trade_choice'] = bktest_df['geo_trade_choice'].str[:2]
    bktest_df['mapped_fut_contract'] = bktest_df['geo_trade_choice'].map(geo_settings)
    # Creating price mask
    bktest_df['mapped_fut_contract_price'] = get_masked_values(bktest_df, 'mapped_fut_contract')
    bktest_df['fut_currency'] = 'GBP' + bktest_df['mapped_fut_contract'].map(universe['currency']) + ' Curncy'
    bktest_df['fut_mul'] = bktest_df['mapped_fut_contract'].map(universe['contract_mul'])
    bktest_df['fut_fx_rate'] = get_masked_values(bktest_df, 'fut_currency')


def append_hedging_txs(bktest_df: DataFrame):
    notional = notional_per_trade_gbp * bktest_df['fut_fx_rate']
    bktest_df['fut_contract_txs'] = notional / (bktest_df['fut_mul'] * bktest_df['mapped_fut_contract_price'])
    bktest_df['fut_contract_txs'] = -(bktest_df['fut_contract_txs'].round())
    bktest_df['total_notional_local_fx'] = bktest_df['fut_contract_txs'].abs() * \
                                           bktest_df['mapped_fut_contract_price'] * bktest_df['fut_mul']
    bktest_df['total_notional_gbp'] = bktest_df['total_notional_local_fx'] / bktest_df['fut_fx_rate']
    bktest_df['notional_margin_gbp'] = bktest_df['total_notional_gbp'] * ltv
    bktest_df['notional_cash_gbp'] = bktest_df['total_notional_gbp'] - bktest_df['notional_margin_gbp']
    bktest_df['fx_contract_txs_value'] = bktest_df['fut_currency'] + ' val_price'
    fx_val_prices = get_masked_values(bktest_df, 'fx_contract_txs_value')
    bktest_df['fx_contract_txs'] = bktest_df['total_notional_gbp'].copy()
    bktest_df['fx_contract_txs_value'] = -(fx_val_prices * bktest_df['fx_contract_txs'])
    bktest_df['fx_contract_txs_price'] = fx_val_prices


def append_txs_signal_final(bktest_df: DataFrame, geo_settings: dict = geo_setting, **kwargs):
    open_signal_suffix = '_open_txs_signal'
    for geo in geo_settings:
        bktest_df[geo + '_open_txs_signal_final'] = bktest_df[geo + open_signal_suffix].where(
            bktest_df['geo_trade_choice'].eq(geo), 0)

def get_no_of_trading_days(bktest_df: DataFrame, col_name_suffix: str, open_signal_number: int, geo: str):
    opn_txs_signal_final = bktest_df[geo + col_name_suffix].copy()
    dates_numpy = bktest_df['trading_date_utc'].values.astype('<M8[D]')
    opn_signal_days = bktest_df['trading_date_utc'].where(opn_txs_signal_final.eq(open_signal_number)).fillna(
        method='ffill')
    opn_signal_days[opn_signal_days.isnull()] = dates_numpy[opn_signal_days.isnull()]
    opn_signal_days = opn_signal_days.values.astype('<M8[D]')

    no_of_tdays_between_opn_sig_array = busday_count(opn_signal_days, dates_numpy, holidays=lse_holidays_array)
    result = Series(no_of_tdays_between_opn_sig_array, index=bktest_df.index)
    return result.replace(0, nan)

def get_one_position_close_txs_signal(opn_signal_number, opn_txs_signal_final, close_txs_signal):
    working_mask = opn_signal_number.where(opn_txs_signal_final.ne(opn_txs_signal_final.shift(1)) & opn_txs_signal_final.eq(1))
    mask = working_mask.fillna(method='ffill').where(opn_txs_signal_final.eq(1))
    mask = mask.fillna(method='ffill')
    final_mask = close_txs_signal.ge(mask)
    result = close_txs_signal.where(final_mask)
    return result

def sum_of_opn_signals(df, t, opn_signal_cols):
    return df[opn_signal_cols].iloc[t].sum()

def date_of_last_opn(df, t):
    if df['can_buy'][t] == 1:
        return df.index.date[t]
    else:
        return df['prev_opn_signal_dt'][t-1]

def no_of_days_since_prev_opn(df, t):
    if isnull(df['prev_opn_signal_dt'][t]):
        return nan
    else:
        start_dt = df['prev_opn_signal_dt'][t].to_numpy().astype('<M8[D]')
        end_dt = datetime64(df.index.date[t])
        result = busday_count(start_dt, end_dt, holidays=lse_holidays_array)
        return result

def position_at_end_geo(df, t, opn_signal_cols):
    if df['txs_signal'][t] == 1:
        return ((df[opn_signal_cols].iloc[t, :] == 1).idxmax())[:2]
    elif df['txs_signal'][t] == 0:
        return df['pos_at_end_geo'][t-1]
    else:
        return nan

def have_prev_geo_and_close_signal(df, t):
    if df['position_signal_start'][t] == 0:
        return False
    else:
        existing_pos_geo = df['pos_at_end_geo'][t-1]
        pos_close_signal = df[existing_pos_geo + '_close_txs_signal'][t]
        if pos_close_signal == 1:
            return True
        else:
            return False

def txs_signal(df, t):
    if df['can_buy'][t] == 1:
        return 1
    elif df['can_sell'][t] == 1:
        return -1
    else:
        return 0

def append_pos_txs_signals(bktest_df: DataFrame, geo_settings: dict = geo_setting,
                           minimum_holding_period_days: int = min_hold_period_days, **kwargs):
    opn_txs_signals_col = []
    close_txs_signals_col = []
    for geo in geo_settings:
        opn_txs_signals_col.append(geo + '_open_txs_signal_final')
        close_txs_signals_col.append(geo + '_close_txs_signal')
    list_of_headings = opn_txs_signals_col + close_txs_signals_col
    df = bktest_df[list_of_headings].copy()
    cols_to_append = ['can_buy', 'can_sell', 'have_close_signal_and_prev_geo', 'pos_at_end_geo',
        'prev_opn_signal_dt', 'no_of_days_since_opn', 'longer_than_min_hold_period',
        'position_signal_start', 'position_signal_end', 'txs_signal']
    for col in cols_to_append:
        df[col] = nan

    txs_time_london = kwargs['earliest_trading_time_london']['ES1 Index'].strftime('%H:%M:%S')
    df.index = df.index.tz_convert('Europe/London')
    df = df.at_time(txs_time_london)

    col_name_idx_map = {}
    for idx, name in enumerate(df.columns):
        col_name_idx_map[name] = idx

    new_appended_idx = df.index.min() - Timedelta(days=1)
    df.loc[new_appended_idx, :] = nan
    df = df.sort_index()
    no_of_t_pts = len(df.index)
    df.loc[:, 'prev_opn_signal_dt'] = NaT
    for t in range(1, no_of_t_pts):
        if t == 1:
            df.iloc[t, col_name_idx_map['position_signal_start']] = 0
        else:
            df.iloc[t, col_name_idx_map['position_signal_start']] = df.iloc[t-1, col_name_idx_map['position_signal_end']]
        df.iloc[t, col_name_idx_map['can_buy']] = (df.iloc[t, col_name_idx_map['position_signal_start']] == 0) & (sum_of_opn_signals(df, t, opn_txs_signals_col) == 1)
        df.iloc[t, col_name_idx_map['prev_opn_signal_dt']] = date_of_last_opn(df, t)
        df.iloc[t, col_name_idx_map['no_of_days_since_opn']] = no_of_days_since_prev_opn(df, t)
        df.iloc[t, col_name_idx_map['longer_than_min_hold_period']] = True if df.iloc[t, col_name_idx_map['no_of_days_since_opn']] > minimum_holding_period_days else False
        df.iloc[t, col_name_idx_map['have_close_signal_and_prev_geo']] = have_prev_geo_and_close_signal(df, t)
        df.iloc[t, col_name_idx_map['can_sell']] = (df.iloc[t, col_name_idx_map['have_close_signal_and_prev_geo']]) * (df.iloc[t, col_name_idx_map['longer_than_min_hold_period']])
        df.iloc[t, col_name_idx_map['txs_signal']] = txs_signal(df, t)
        df.iloc[t, col_name_idx_map['pos_at_end_geo']] = position_at_end_geo(df, t, opn_txs_signals_col)
        df.iloc[t, col_name_idx_map['position_signal_end']] = df.iloc[t, col_name_idx_map['position_signal_start']] + df.iloc[t, col_name_idx_map['txs_signal']]

    for geo in geo_settings:
        bktest_df.loc[:, geo + '_position_signal'] = df['position_signal_end'].where(df['pos_at_end_geo'].eq(geo), 0).reindex(index=bktest_df.index).fillna(method='ffill').fillna(0)
        bktest_df.loc[:, geo + '_txs_signal'] = bktest_df[geo + '_position_signal'].diff()

    return df

def append_position_signals_vec(bktest_df: DataFrame, geo_settings: dict = geo_setting,
                                minimum_holding_period_days: int = min_hold_period_days, **kwargs):
    if len(geo_settings) > 1 and kwargs['one_position'] is True:
        append_pos_txs_signals(bktest_df, geo_settings, minimum_holding_period_days, **kwargs)
    else:
        for geo in geo_settings:
            geo_string = geo + '_open_txs_signal_final'
            close_txs_signal_mask = bktest_df[geo_string].cumsum()
            bktest_df.loc[:, geo + '_close_txs_signal_final'] = close_txs_signal_mask.copy()
            bktest_df.loc[:, geo + '_close_txs_signal_final'] = bktest_df[geo + '_close_txs_signal_final'].where(
                bktest_df[geo + '_close_txs_signal_final'].ne(bktest_df[geo + '_close_txs_signal_final'].shift(1)), 0)
            bktest_df.loc[:, geo + '_close_txs_signal_mask'] = nan
            last_open = close_txs_signal_mask.max()
            if last_open == 0:
                continue
            else:
                i = 1
                while i <= last_open:
                    no_of_trade_days_since = get_no_of_trading_days(bktest_df, '_close_txs_signal_final', i, geo)
                    if i < last_open:
                        numpy_day_start = close_txs_signal_mask.index[
                            close_txs_signal_mask.eq(i).values.argmax()].to_numpy().astype('<M8[D]')
                        numpy_day_end = close_txs_signal_mask.index[
                            close_txs_signal_mask.eq(i + 1).values.argmax()].to_numpy().astype('<M8[D]')
                        no_of_tdays_between_opn_sig = busday_count(numpy_day_start, numpy_day_end, holidays=lse_holidays_array)
                        opn_sig_mask = no_of_trade_days_since.le(
                            no_of_tdays_between_opn_sig + minimum_holding_period_days - 1) & \
                                       no_of_trade_days_since.gt(minimum_holding_period_days - 1)
                    else:
                        opn_sig_mask = no_of_trade_days_since.gt(minimum_holding_period_days - 1)

                    bktest_df[geo + '_close_txs_signal_mask'].mask(opn_sig_mask, i, inplace=True)

                    i = i + 1

            bktest_df.loc[:, geo + '_close_txs_signal_final'] = bktest_df[geo + '_close_txs_signal'].multiply(
                bktest_df[geo + '_close_txs_signal_mask'])
            bktest_df.loc[:, geo + '_close_txs_signal_final'] = bktest_df[geo + '_close_txs_signal_final'].where(
                ~bktest_df[geo + '_close_txs_signal_final'].duplicated())

            if kwargs['one_position'] is True:
                bktest_df.loc[:, geo + '_close_txs_signal_final'] = get_one_position_close_txs_signal(
                    close_txs_signal_mask, bktest_df[geo + '_open_txs_signal_final'], bktest_df[geo + '_close_txs_signal_final'])

            bktest_df.loc[:, geo + '_position_signal'] = close_txs_signal_mask - bktest_df[
                geo + '_close_txs_signal_final'].fillna(method='ffill').fillna(0)
            bktest_df.loc[:, geo + '_txs_signal'] = bktest_df[geo + '_position_signal'].diff()


def get_fund_list_based_on_geo(geo: str, universe_df: DataFrame):
    universe_df = universe_df.set_index('symbol')
    fund_list = list(universe_df.index[universe_df['geo'].eq(geo) & universe_df['asset_type'].eq('fund')])
    return fund_list


def append_partial_closing_txs(backtest_df: DataFrame, geo_settings: dict):
    cols = ['fut_contract_txs', 'fx_contract_txs', 'fut_currency', 'mapped_fut_contract']
    for geo, fut in geo_settings.items():
        opn_signal_final = geo + '_open_txs_signal_final'
        close_signal_final = geo + '_close_txs_signal_final'
        opn_signal_txs_by_no_w = backtest_df[opn_signal_final].cumsum().where(
            ~backtest_df[opn_signal_final].cumsum().duplicated())
        opn_signal_txs_by_no = DataFrame(index=opn_signal_txs_by_no_w.fillna(0).values, columns=cols)
        opn_signal_txs_by_no['fut_contract_txs'] = -1 * backtest_df['fut_contract_txs'].where(
            opn_signal_txs_by_no_w.notnull()).values
        opn_signal_txs_by_no['fx_contract_txs'] = -1 * backtest_df['fx_contract_txs'].where(
            opn_signal_txs_by_no_w.notnull()).values
        opn_signal_txs_by_no = opn_signal_txs_by_no.cumsum()
        opn_signal_txs_by_no['mapped_fut_contract'] = backtest_df['mapped_fut_contract'].where(
            opn_signal_txs_by_no_w.notnull()).values
        opn_signal_txs_by_no['fut_currency'] = backtest_df['fut_currency'].where(
            opn_signal_txs_by_no_w.notnull()).values
        opn_signal_txs_by_no = opn_signal_txs_by_no.dropna(how='any')
        opn_signal_txs_by_no.loc[0, :] = [0, 0 , geo_fx_map[geo], fut]
        partial_close_mask = backtest_df[close_signal_final].where(
            backtest_df[opn_signal_final].cumsum() > backtest_df[close_signal_final])
        partial_close_mask_df = DataFrame(index=backtest_df.index, columns=['fut_contract_txs', 'fx_contract_txs',
                                                                            'fut_currency', 'mapped_fut_contract'])
        start_event_no = partial_close_mask + backtest_df[geo + '_txs_signal']
        for type in ['fut_contract_txs', 'fx_contract_txs']:
            partial_close_mask_df.loc[:, type] = opn_signal_txs_by_no.reindex(partial_close_mask.values)[type].values - opn_signal_txs_by_no.reindex(start_event_no.values)[type].values
        for asset in ['fut_currency', 'mapped_fut_contract']:
            partial_close_mask_df.loc[:, asset] = opn_signal_txs_by_no.reindex(partial_close_mask.values)[asset].values
        backtest_df.loc[:, cols] = backtest_df[cols].combine_first(partial_close_mask_df)

def combine_position_txs_signals(bktest_df: DataFrame, geo_settings: dict=geo_setting):
    pos_signal_df = DataFrame(index=bktest_df.index)
    txs_signal_df = DataFrame(index=bktest_df.index)
    for geo in geo_settings:
        pos_signal_df.loc[:, geo + '_position_signal'] = bktest_df[geo + '_position_signal'].copy()
        txs_signal_df.loc[:, geo + '_txs_signal'] = bktest_df[geo + '_txs_signal'].copy()
    return pos_signal_df.sum(axis='columns'), txs_signal_df.sum(axis='columns')

def adjust_equity_trade_date(bktest_df: DataFrame):
    bktest_df['equity_trade_str_col'] = bktest_df['mapped_fut_contract'] + '_equity_trading_date_local'
    bktest_df['equity_trade_date_local'] = get_masked_values(bktest_df, 'equity_trade_str_col', equity=True)

def get_net_weights(geo_settings: dict, prop_weights: DataFrame, universe: DataFrame):
    prop_weights = prop_weights.set_index('Symbol')
    result_weights_list = []
    for geo, fut in geo_settings.items():
        fund_list = get_fund_list_based_on_geo(geo, universe)
        # assuming other equites are 0% in the funds
        result_weights_list.append(-((prop_weights[fund_list].fillna(0) / len(fund_list)).subtract(prop_weights[fut], axis='index')))
    net_weights = concat(result_weights_list, axis='columns')
    return net_weights

def get_equity_pos_by_funds(working_pos_df: DataFrame, net_weights: DataFrame, fund_list: list):
    geo_net_weights = net_weights[fund_list].dropna(how='all')
    for index, fund in enumerate(fund_list):
        weight_working = geo_net_weights[fund].to_frame().transpose()
        weight_working = DataFrame(weight_working.values, index=[working_pos_df.index[0]],
                                   columns=weight_working.columns).reindex(index=working_pos_df.index).fillna(method='ffill')
        weight_working = weight_working.multiply(working_pos_df[fund], axis='index').fillna(0)
        if index == 0:
            sum_df = weight_working
        else:
            sum_df = sum_df + weight_working
    return sum_df

def get_equity_list_by_funds(net_weights: DataFrame, fund_list: list):
    return list(net_weights[fund_list].dropna(how='all').index)

def get_one_pos_txs_mask(bktest_df, combined_pos_signal, geo_settings: dict=geo_setting):
    result_df = DataFrame(index=bktest_df.index)
    for geo in geo_settings:
        geo_clean_mask = combined_pos_signal - bktest_df[geo + '_position_signal']
        txs_sig = bktest_df[geo + '_txs_signal'].mask(bktest_df[geo + '_txs_signal'].lt(0) & geo_clean_mask.gt(0), 0)
        result_df.loc[:, geo + '_txs_signal'] = txs_sig
    return result_df.sum(axis='columns').replace(0, nan).fillna(method='ffill').fillna(0)

def get_txs_pos_dfs(backtest_df: DataFrame, daily_prices: DataFrame, universe: DataFrame, geo_settings: dict, **kwargs):
    equity_trade = kwargs.get('equity_trade', False)
    one_position = kwargs.get('one_position', True)
    if equity_trade is True:
        adjust_equity_trade_date(backtest_df)
        net_weights = get_net_weights(geo_settings, proportional_weights, universe)
    if one_position is False:
        # This currently only support one geo
        append_partial_closing_txs(backtest_df, geo_settings)
    else:
        pos_signal_series, txs_signal_series = combine_position_txs_signals(backtest_df, geo_settings)
        txs_signal_series = txs_signal_series.mask(txs_signal_series.lt(0), -1)

    future_contracts = backtest_df['mapped_fut_contract'].unique().astype(str)
    future_contracts = future_contracts[future_contracts != 'nan']
    fx_contracts = backtest_df['fut_currency'].unique().astype(str)
    fx_contracts = fx_contracts[fx_contracts != 'nan']
    universe_s = universe.set_index('symbol')
    funds = universe_s[universe_s['asset_type'].eq('fund')].index.to_numpy()
    product_list = list(concatenate([future_contracts, fx_contracts, funds]))
    position_df = DataFrame(index=backtest_df.index, columns=product_list)
    txs_df = DataFrame(index=backtest_df.index, columns=product_list)

    position_df.loc[:, 'trading_date_utc'] = backtest_df['trading_date_utc']
    txs_df.loc[:, 'trading_date_utc'] = backtest_df['trading_date_utc']
    for geo, fut in geo_settings.items():
    # ================================================ constraints =================================================
        if one_position is True:
            one_pos_txs_s = txs_signal_series.replace(0, nan).fillna(method='ffill').fillna(0)
            position_mask = one_pos_txs_s.ge(1) & one_pos_txs_s.ne(one_pos_txs_s.shift(1).fillna(0))
        else:
            position_mask = backtest_df[geo + '_position_signal'].ge(1)

        fx = geo_fx_map[geo]
        position_df.loc[:, fut] = backtest_df['fut_contract_txs'].where(backtest_df['mapped_fut_contract'].eq(fut),
                                                                        0).where(position_mask, nan)
        position_df.loc[:, fx] = backtest_df['fx_contract_txs'].where(backtest_df['fut_currency'].eq(fx),
                                                                      0).where(position_mask, nan)
        if one_position is True:
            position_df.loc[:, fut] = position_df[fut].fillna(0).where(one_pos_txs_s.eq(1), nan)
            position_df.loc[:, fx] = position_df[fx].fillna(0).where(one_pos_txs_s.eq(1), nan)
    # =============================================================================================================

        # choosing funds - this can be dynamic in the future ====================================================
        fund_list = get_fund_list_based_on_geo(geo, universe)
        for fund in fund_list:
            position_df.loc[:, fund] = position_df[fx] / len(fund_list)
        if equity_trade is True:
            equity_pos = get_equity_pos_by_funds(position_df, net_weights, fund_list)
            equity_pos = equity_pos.where(position_df[fut].notnull(), nan)
            equity_list = list(equity_pos.columns)
            # Converting local equity price to GBP
            equity_trade_price = daily_prices.reindex(index=backtest_df[fut + '_equity_trading_date_local'])[equity_list]
            equity_trade_price.index = position_df.index
            equity_trade_price = equity_trade_price.divide(backtest_df[fx + ' equity_price'].fillna(method='ffill'), axis='index').fillna(method='ffill')
            equity_pos = equity_pos.divide(equity_trade_price)
            position_df = position_df.join(equity_pos)
            for equity in equity_pos:
                position_df.loc[:, equity] = position_df[equity].groupby(position_df[equity].isna().cumsum()).cumsum().fillna(0)
            txs_df = txs_df.join(position_df[equity_list].diff())
        fund_trade_prices = daily_prices.reindex(index=position_df['trading_date_utc'])[fund_list]
        fund_trade_prices.index = position_df.index
        position_df.loc[:, fund_list] = position_df[fund_list].divide(
            fund_trade_prices.fillna(method='ffill') / 100)  # assuming all fund prices are pense
        # =======================================================================================================

        for fund in fund_list:
            position_df.loc[:, fund] = position_df[fund].groupby(position_df[fund].isna().cumsum()).cumsum().fillna(0)
        position_df.loc[:, fut] = position_df[fut].groupby(position_df[fut].isna().cumsum()).cumsum().fillna(0)
        position_df.loc[:, fx] = position_df[fx].groupby(position_df[fx].isna().cumsum()).cumsum().fillna(0)
        txs_df.loc[:, fut] = position_df[fut].diff()
        txs_df.loc[:, fx] = position_df[fx].diff()
        txs_df.loc[:, fund_list] = position_df[fund_list].diff()

    return {'position': position_df, 'txs': txs_df}


def get_product_map(df: DataFrame, map_type: str, universe_df: DataFrame):
    universe_df = universe_df.set_index('symbol')
    map = DataFrame(index=df.index, columns=df.columns)
    for columns in df:
        map[columns] = universe_df.loc[columns, map_type]
    return map


def get_txs_cal_df(bktest_df: DataFrame, txs_df: DataFrame, daily_prices: DataFrame, universe: DataFrame, 
                   geo_settings: dict, equity_trade: bool=False, **kwargs):
    if equity_trade is True:
        net_weights = get_net_weights(geo_settings, proportional_weights, universe)
    trade_price_df = DataFrame(index=txs_df.index, columns=txs_df.columns)
    for geo, fut in geo_settings.items():
        fx = geo_fx_map[geo]
        trade_price_df.loc[:, fut] = bktest_df[fut].fillna(method='ffill').where(txs_df[fut].ne(0))
        trade_price_df.loc[:, fx] = bktest_df[fx + ' val_price'].fillna(method='ffill').where(txs_df[fut].ne(0))
        fund_list = get_fund_list_based_on_geo(geo, universe)
        fund_trade_prices = daily_prices.reindex(index=txs_df['trading_date_utc'])[fund_list]
        fund_trade_prices.index = txs_df.index
        trade_price_df.loc[:, fund_list] = fund_trade_prices.fillna(method='ffill').where(txs_df[fund_list].ne(0))

        if equity_trade is True:
            equity_list = get_equity_list_by_funds(net_weights, fund_list)
            equity_trade_price = daily_prices.reindex(index=bktest_df[fut + '_equity_trading_date_local'])[equity_list]
            equity_trade_price.index = txs_df.index
            trade_price_df.loc[:, equity_list] = equity_trade_price.fillna(method='ffill').where(txs_df[equity_list].ne(0))

    contract_mul_map = get_product_map(trade_price_df.drop('trading_date_utc', axis='columns'), 'contract_mul', universe)

    return -1 * (txs_df.drop('trading_date_utc', axis='columns'
                             ).multiply(trade_price_df.drop('trading_date_utc', axis='columns')).multiply(
        contract_mul_map))


def get_position_cal_df(bktest_df: DataFrame, position_df: DataFrame, daily_prices: DataFrame, universe: DataFrame, 
                        geo_settings: dict, equity_trade: bool=False, **kwargs):
    if equity_trade is True:
        net_weights = get_net_weights(geo_settings, universe)
    mark_price_df = DataFrame(index=position_df.index, columns=position_df.columns)
    for geo, fut in geo_settings.items():
        fx = geo_fx_map[geo]
        mark_price_df.loc[:, fut] = bktest_df[fut + '_ref_time_price'].fillna(method='ffill').where(
            position_df[fut].ne(0))
        mark_price_df.loc[:, fx] = bktest_df[fx + ' val_price'].fillna(method='ffill').where(position_df[fut].ne(0))
        fund_list = get_fund_list_based_on_geo(geo, universe)
        fund_trade_prices = daily_prices.reindex(index=position_df['trading_date_utc'])[fund_list]
        fund_trade_prices.index = position_df.index
        mark_price_df.loc[:, fund_list] = fund_trade_prices.fillna(method='ffill').where(position_df[fund_list].ne(0))
        if equity_trade is True:
            equity_list = get_equity_list_by_funds(net_weights, fund_list)
            equity_mark_price = daily_prices.reindex(index=bktest_df[fut + '_equity_trading_date_local'])[equity_list]
            equity_mark_price.index = position_df.index
            mark_price_df.loc[:, equity_list] = equity_mark_price.fillna(method='ffill').where(position_df[equity_list].ne(0))
    contract_mul_map = get_product_map(mark_price_df.drop('trading_date_utc', axis='columns'), 'contract_mul', universe)

    return position_df.drop('trading_date_utc', axis='columns'
                            ).multiply(mark_price_df.drop('trading_date_utc', axis='columns')).multiply(
        contract_mul_map)


def get_txs_costs(txs_cal_df: DataFrame, universe, geo_settings: dict, equity_trade: bool=False, **kwargs):
    if equity_trade is True:
        net_weights = get_net_weights(geo_settings, proportional_weights, universe)
        list_of_equities = []
    txs_cost = txs_cal_df.copy()
    list_of_fx = []
    list_of_funds = []
    for geo, fut in geo_settings.items():
        list_of_fx.append(geo_fx_map[geo])
        fund_list_by_geo = get_fund_list_based_on_geo(geo, universe)
        list_of_funds = list_of_funds + fund_list_by_geo
        if equity_trade is True:
            equity_list = get_equity_list_by_funds(net_weights, fund_list_by_geo)
            list_of_equities = list_of_equities + equity_list
            txs_cost.loc[:, list_of_equities] = 0
        future_cost = txs_cost[fut].where(txs_cost[fut].isnull(), future_spread_cost_as_percent[fut]) * txs_cost[
            fut].abs()
        txs_cost.loc[:, fut] = future_cost
    fund_cost = txs_cost[list_of_funds].where(txs_cost[list_of_funds].isnull(), fund_dealing_charge_percent) * txs_cost[
        list_of_funds].abs()
    txs_cost.loc[:, list_of_funds] = fund_cost
    fx_cost = txs_cost[list_of_fx].where(txs_cost[list_of_fx].isnull(), fx_spread_cost_as_percent) * txs_cost[
        list_of_fx].abs()
    txs_cost.loc[:, list_of_fx] = fx_cost
    return txs_cost


def get_net_txs_val(txs_cal_df: DataFrame, universe: DataFrame, geo_settings: dict, **kwargs):
    txs_cost = get_txs_costs(txs_cal_df, universe, geo_settings,  **kwargs)
    return txs_cal_df - txs_cost


def get_mapped_fx_rates(fx_map: DataFrame, bktest_df: DataFrame):
    list_of_fx = fx_map.iloc[0, :].unique()
    for i in range(len(list_of_fx)):
        fx_pair_name = 'GBP' + list_of_fx[i] + ' Curncy'
        fx_map = fx_map.replace(list_of_fx[i], fx_pair_name)
        list_of_fx[i] = fx_pair_name
    for columns in fx_map:
        fx = fx_map[columns][0]
        if fx == 'GBPGBp Curncy':
            fx_map.loc[:, columns] = 100
        else:
            fx_map.loc[:, columns] = bktest_df[fx + ' val_price'].reindex(index=fx_map.index)
    return fx_map


def get_pnl_local(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe: DataFrame, **kwargs):
    txs_value = get_daily_net_txs_val(bktest_df, txs_pos_dfs, daily_prices, universe, **kwargs)
    value_at_end = get_daily_value_at_end(bktest_df, txs_pos_dfs, daily_prices, universe, **kwargs)

    pnl_local = value_at_end.fillna(0).diff() + txs_value.fillna(0)
    pnl_local.index = pnl_local.index.tz_convert('UTC')
    return pnl_local

def get_daily_txs(txs_pos_dfs: dict, **kwargs):
    txs_time_london = kwargs['earliest_trading_time_london']['ES1 Index'].strftime('%H:%M:%S')
    txs_df = txs_pos_dfs['txs'].copy()
    txs_df.index = txs_df.index.tz_convert('Europe/London')
    return txs_df.at_time(txs_time_london)

def get_daily_position(txs_pos_dfs: dict, **kwargs):
    txs_time_london = kwargs['earliest_trading_time_london']['ES1 Index'].strftime('%H:%M:%S')
    position_df = txs_pos_dfs['position'].copy()
    position_df.index = position_df.index.tz_convert('Europe/London')
    return position_df.at_time(txs_time_london)


def get_daily_net_txs_val(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe: DataFrame, **kwargs):
    # Default time for reporting =================================================
    txs_time_london = kwargs['earliest_trading_time_london']['ES1 Index'].strftime('%H:%M:%S')
    # ============================================================================
    txs_value = get_txs_cal_df(bktest_df, txs_pos_dfs['txs'], daily_prices, universe, **kwargs)
    txs_value = get_net_txs_val(txs_value, universe, **kwargs)
    txs_value.index = txs_value.index.tz_convert('Europe/London')
    txs_value = txs_value.at_time(txs_time_london)
    return txs_value

def get_daily_value_at_end(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe, **kwargs):
    txs_time_london = kwargs['earliest_trading_time_london']['ES1 Index'].strftime('%H:%M:%S')
    value_at_end = get_position_cal_df(bktest_df, txs_pos_dfs['position'], daily_prices, universe, **kwargs)
    value_at_end.index = value_at_end.index.tz_convert('Europe/London')
    value_at_end = value_at_end.at_time(txs_time_london)
    return value_at_end

def get_daily_net_txs_val_gbp(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe: DataFrame, **kwargs):
    txs_value = get_daily_net_txs_val(bktest_df, txs_pos_dfs, daily_prices, universe, **kwargs)
    txs_value.index = txs_value.index.tz_convert('UTC')
    txs_value_map = get_product_map(txs_value, 'currency', universe)
    txs_value_gbp_rate_map = get_mapped_fx_rates(txs_value_map, bktest_df)
    result = txs_value.divide(txs_value_gbp_rate_map)
    return result

def get_daily_value_at_end_gbp(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe, **kwargs):
    value_at_end = get_daily_value_at_end(bktest_df, txs_pos_dfs, daily_prices, universe, **kwargs)
    value_at_end.index = value_at_end.index.tz_convert('UTC')
    value_at_end_map = get_product_map(value_at_end, 'currency', universe)
    value_at_end_gbp_rate_map = get_mapped_fx_rates(value_at_end_map, bktest_df)
    result = value_at_end.divide(value_at_end_gbp_rate_map)
    return result


def get_pnl_gbp(bktest_df: DataFrame, txs_pos_dfs: dict, daily_prices: DataFrame, universe: DataFrame, **kwargs):
    pnl_local = get_pnl_local(bktest_df, txs_pos_dfs, daily_prices, universe, **kwargs)
    pnl_fx_map = get_product_map(pnl_local, 'currency', universe)
    pnl_gbp_rate_map = get_mapped_fx_rates(pnl_fx_map, bktest_df)
    pnl_gbp = pnl_local.divide(pnl_gbp_rate_map)
    return pnl_gbp

def get_event_return(bktest_df: DataFrame, position_df: DataFrame, daily_prices: DataFrame,
                     geo_settings: dict = geo_setting):
    # Index by Event Number, columns such as start time, end time, future pnl, fx pnl and fund pnl
    pass

def get_total_daily_pnl(pnl: DataFrame):
    pnl['Date'] = pnl.index.date
    daily_pnl_gbp = pnl.groupby('Date').sum()
    return daily_pnl_gbp.sum(axis='columns')


def get_perf_matrix(total_cum_pnl_gbp: DataFrame, starting_capital: float = starting_capital_gbp):
    daily_returns = (total_cum_pnl_gbp + starting_capital).pct_change()
    annual_return = daily_returns.mean() * 225
    annual_std = daily_returns.std() * (225 ** 0.5)
    sharpe = annual_return / annual_std
    return {'Annualised Return': annual_return, 'Annualised Std': annual_std, 'Sharpe': sharpe}

def get_complete_backtest_df(bktest_df: DataFrame, universe: DataFrame, **kwargs):
    bktest_df = get_trading_time_backtest_df(bktest_df)
    append_txs_signals(bktest_df, **kwargs)
    append_opn_trade_indicators(bktest_df, universe, **kwargs)
    append_hedging_txs(bktest_df)
    append_txs_signal_final(bktest_df, **kwargs)
    append_pos_txs_signals(bktest_df, **kwargs)
    return bktest_df

def construct_q_object(fut_prefix, other_symbols, price=False, start_dt=None, end_dt=None):
    others_q = Q(symbol__in=other_symbols)
    future_startswith_q = reduce(or_, [Q(symbol__startswith=pref) for pref in fut_prefix])
    if price:
        general_q = Q(time__range=[start_dt, end_dt], aspect__in=[open, high, low, close], resolution='5m')
        q_query = general_q & (future_startswith_q | others_q)
    else:
        q_query = future_startswith_q | others_q
    return q_query

def get_query_symbols(configs):
    futures = list(configs['geo_settings'].values())
    geos = list(configs['geo_settings'].keys())
    indices = [underlying_index[f] for f in futures]
    future_prefix = [INDEX_TO_FUTURE_PREFIX[idx] for idx in indices]
    fx = [geo_fx_map[g] for g in geos]
    volatility_symbols = [INDEX_TO_VOL_MAP[idx] for idx in indices]
    return {'future_prefix': future_prefix, 'fx': fx, 'vol': volatility_symbols,
            'index': indices, 'fund': configs['funds'], 'future': futures}


# @cache_result(timeout_s=24*60*60)
def get_bar_price(symbol_dic, start_dt, end_dt, **configs):
    future_prefix = symbol_dic['future_prefix']
    idx_fx_symbols = symbol_dic['fx'] + symbol_dic['index']
    bar_price_query_set = construct_q_object(future_prefix, idx_fx_symbols, price=True,
                                             start_dt=start_dt, end_dt=end_dt)
    bar_price = read_frame(Price.objects.filter(bar_price_query_set))
    return bar_price

# @cache_result(timeout_s=24*60*60)
def get_daily_price(symbol_dic, start_dt, end_dt, **configs):
    list_of_daily_symbols = symbol_dic['index'] + symbol_dic['vol'] + symbol_dic['fund']
    df = read_prices(time__range=[start_dt, end_dt], symbol__in=list_of_daily_symbols,
                     aspect = close, resolution=one_day, price_type__in=[trade, nav])
    df = df.pivot_table(index='time', columns='symbol', values='value')
    return df

# @cache_result(timeout_s=24*60*60)
def get_universe(symbol_dic):
    future_prefix = symbol_dic['future_prefix']
    list_of_other_symbols = symbol_dic['index'] + symbol_dic['vol'] + symbol_dic['fund'] + symbol_dic['fx']
    qs = construct_q_object(future_prefix, list_of_other_symbols)
    universe = read_frame(Universe.objects.filter(qs))
    return universe

def get_origin_data_dic(symbol_dic, **configs):
    bar_price = get_bar_price(symbol_dic, **configs)
    daily_price = get_daily_price(symbol_dic, **configs)
    universe = get_universe(symbol_dic)
    universe.loc[:, 'contract_mul'] = universe['contract_mul'].fillna(1)
    keys = ['fut_bar_p', 'idx_bar_p', 'fx_bar_p', 'daily_p', 'universe']
    asset_type_dic = {'fut_bar_p': index_future, 'idx_bar_p': equity_index, 'fx_bar_p': fx_spot}
    result_dic = dict.fromkeys(keys)
    for k, asset_type in asset_type_dic.items():
        result_dic[k] = bar_price[bar_price['asset_type'].eq(asset_type)].copy()
    result_dic['daily_p'] = daily_price
    result_dic['universe'] = universe
    return result_dic

def get_latest_sim_result(is_live=True, **kwargs):
    dic = {'is_live': is_live}
    dic = {**dic, **kwargs}
    df = read_frame(SimResult.objects.filter(**dic))
    df = df.sort_values('as_of', ascending=False)
    df = df.drop_duplicates(subset=['geo', 'index', 'volatility_index', 'start_date', 'end_date',
                                    'fund', 'min_holding_days', 'dealing_cutoff_time_london'], keep='first')
    df = df.drop('id', axis='columns')
    no_of_records = len(df)
    list_of_factor_names = ['open_threshold_beta', 'open_threshold_alpha', 'close_threshold_beta',
                            'close_threshold_alpha']
    result_dic = dict.fromkeys(list_of_factor_names)
    if no_of_records > 1:
        raise Exception('More than 1 set of threshold factors is returned.')
    elif no_of_records == 0:
        raise Exception('No threshold factor record found.')
    else:
        for name in list_of_factor_names:
            result_dic[name] = df[name].values[0]
    return result_dic


if __name__ == '__main__':
    #configs = {'opening_signal_threshold': 0.006, 'closing_signal_threshold': -0.006,
    #           'dealing_cutoff_time_london': time(11, 0, 0),
    #           'earliest_trading_time_london': {'TP1 Index': time(10, 55, 0), 'ES1 Index': time(10, 55, 0)},
    #           'minimum_holding_period_days': 10, 'vol_based_thresholds': True, 'open_threshold_factors': [0.00025, 0],
    #           'close_threshold_factors': [-0.00025, 0], 'equity_trade': False, 'start_dt': '2014-10-01',
    #           'end_dt': '2020-03-01', 'geo_settings': {'US': 'ES1 Index'}, 'one_position': True,
    #           'funds': ['JPMUEAA LN Equity', 'HSBAMCA LN Equity']}

    #symbol_dic = get_query_symbols(configs)
    #dic = get_origin_data_dic(symbol_dic, **configs)
    #bar_df = get_bar_price_df(dic, symbol_dic, dic['universe'], **configs)
    #restructured_data = get_restructured_data(bar_df, dic['daily_p'], **configs)
    #data_dic_by_asset = restructured_data[0]
    #daily_price_df = restructured_data[1]

    #add_trading_daytime_cols(data_dic_by_asset, symbol_dic, **configs)
    #backtest_df = create_backtest_df(data_dic_by_asset, **configs)
    #backtest_df = get_complete_backtest_df(backtest_df, dic['universe'], **configs)
    #txs_pos_dic = get_txs_pos_dfs(backtest_df, daily_price_df, dic['universe'], **configs)
    #pnl_gbp = get_pnl_gbp(backtest_df, txs_pos_dic, daily_price_df, dic['universe'], **configs)
    #total_daily_pnl_gbp = get_total_daily_pnl(pnl_gbp)
    #total_cum_pnl_gbp = total_daily_pnl_gbp.cumsum()
    #total_cum_pnl_gbp.plot(grid=True, figsize=(20, 10))
    #perf_matrix = get_perf_matrix(total_cum_pnl_gbp)
    #pass


    print(data)
