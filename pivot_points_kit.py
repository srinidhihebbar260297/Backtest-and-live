import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from kiteconnect import KiteConnect, KiteTicker
import selenium
import time as Time
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pyotp import TOTP
import matplotlib.pyplot as plt

acess_token_data = None
first_sleep = False
nine_fifteen = time(9, 15, 0)
nine_twenty = time(9, 20, 0)
nine_thirty = time(9, 30, 0)
three_fifteen = time(15, 15, 0)
total_ticks = []
api_key = ''
api_secret = ''
totp_code = ''

kite = KiteConnect(api_key=api_key)
pnl = []


def automated_login():

    global acess_token_data
    global access_token
    while acess_token_data is None:
        try:

            driver = webdriver.Chrome()
            driver.get(kite.login_url())
            Time.sleep(2)
            user_id_field = driver.find_element(By.XPATH, '//*[@id="userid"]')
            user_id_field.send_keys('')
            password_field = driver.find_element(
                By.XPATH, '//*[@id="password"]')
            password_field.send_keys('')
            login_button = driver.find_element(
                By.XPATH, '//*[@id="container"]/div/div/div/form/div[4]/button')
            login_button.click()
            Time.sleep(2)
            totp = TOTP(totp_code)
            pin_field = driver.find_element(By.XPATH, '//*[@id="userid"]')
            pin_field.send_keys(totp.now())
            Time.sleep(2)
            request_token = driver.current_url.split(
                'request_token=')[1].split('&action=')[0]

            acess_token_data = kite.generate_session(request_token, api_secret)

        except Exception as error:
            print(error)

        finally:

            # Close the browser
            driver.quit()


automated_login()

access_token = acess_token_data['access_token']

kite.set_access_token(access_token)


instruments_list = pd.DataFrame(kite.instruments('NFO'))
instruments_list = instruments_list[instruments_list['name'].isin(
    ['BANKNIFTY', 'FINNIFTY', 'NIFTY'])]

banknifty_instruments = instruments_list.loc[instruments_list['name'] == 'BANKNIFTY'].sort_values(
    by='expiry')
nifty_instruments = instruments_list.loc[instruments_list['name'] == 'FINNIFTY'].sort_values(
    by='expiry')
finnifty_instruments = instruments_list.loc[instruments_list['name'] == 'NIFTY'].sort_values(
    by='expiry')
banknifty_instruments = banknifty_instruments.loc[banknifty_instruments['expiry']
                                                  == banknifty_instruments.iloc[0]['expiry']]
nifty_instruments = nifty_instruments.loc[nifty_instruments['expiry']
                                          == nifty_instruments.iloc[0]['expiry']]
finnifty_instruments = finnifty_instruments.loc[finnifty_instruments['expiry']
                                                == finnifty_instruments.iloc[0]['expiry']]
instruments_list = pd.concat(
    [banknifty_instruments, finnifty_instruments, nifty_instruments])

calls = instruments_list.loc[instruments_list['tradingsymbol'].str.contains(
    'CE')]
puts = instruments_list.loc[instruments_list['tradingsymbol'].str.contains(
    'PE')]
instrument_zip = dict(
    zip(instruments_list['instrument_token'], instruments_list['tradingsymbol']))
instrument_zip[260105] = 'BANKNIFTY'
instrument_zip[256265] = 'NIFTY'
instrument_zip[257801] = 'FINNIFTY'

instruments_list = instruments_list['instrument_token'].astype('int').to_list()
instruments_list.append(260105)
instruments_list.append(256265)
instruments_list.append(257801)
indices = [260105, 256265, 257801]
# calls=instruments_list
# instruments_list=[260105]

trades_entered = []
trade_check = []
minute_candle = []

# def convert_candles(total_ticks):
#     if datetime.time(datetime.now()).second==0:
#         tick_symbols=pd.DataFrame(total_ticks)['instrument_token'].unique()
#         tick_data=pd.DataFrame(total_ticks.copy())
#         for i in range(len(tick_symbols)):
#             data=tick_data.loc[tick_data['instrument_token']==tick_symbols[i]]
#             candle_data=pd.DataFrame(data).set_index('datetime').loc[:,'price'].resample('1min').ohlc().dropna()
#             candle_data['tradingsymbol']=tick_symbols[i]
#             minute_candle.append(candle_data)
#             minute_candle_data=pd.concat(minute_candle)


def calculate_pivot_points(high, low, close, instrument_symbol):
    pivot = (high + low + close) / 3

    support1 = (2 * pivot) - high
    support2 = pivot - (high - low)
    support3 = low - 2 * (high - pivot)

    resistance1 = (2 * pivot) - low
    resistance2 = pivot + (high - low)
    resistance3 = high + 2 * (pivot - low)

    return {

        'instrument_token': instrument_symbol,
        'pivot': pivot,
        'support1': support1,
        'support2': support2,
        'support3': support3,
        'resistance1': resistance1,
        'resistance2': resistance2,
        'resistance3': resistance3
    }


kws = KiteTicker(api_key, access_token)


indices_pivot_points = []

for i in range(len(indices)):
    historical_data=pd.DataFrame(kite.historical_data(str(indices[i]), datetime.date(datetime.now())-timedelta(30), datetime.date(datetime.now()), '60minute'))
    dates=pd.DataFrame(historical_data['date'].dt.date.unique()).sort_values(by=0)
    if dates.iloc[-1][0]==datetime.date(datetime.now()):
        historical_data=historical_data.loc[historical_data['date'].dt.date==dates.iloc[-2][0]]
    if dates.iloc[-1][0]!=datetime.date(datetime.now()):
        historical_data=historical_data.loc[historical_data['date'].dt.date==dates.iloc[-1][0]]
    historical_data=historical_data.set_index('date').resample('1d').apply({'open':'first','close':'last','high':'max','low':'min'}).reset_index().iloc[-1]
    pivot=calculate_pivot_points( historical_data['high'], historical_data['low'], historical_data['close'], indices[i])
    indices_pivot_points.append(pivot)
    print(pivot)


def on_ticks(ws, ticks):
    # Callback to receive ticks.
    # print(ticks)
    for i in range(len(ticks)):
        datetime = ticks[i]['exchange_timestamp']
        price = ticks[i]['last_price']
        instrument_token = ticks[i]['instrument_token']
        total_ticks.append(dict(datetime=datetime, price=price,
                           instrument_token=instrument_token))
        # convert_candles(total_ticks)


def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    ws.subscribe(instruments_list)

    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, instruments_list)


def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    ws.stop()


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)


# support1,resistance1=pivot['support1'],pivot['resistance1']
#

def pivot_points_trade(total_ticks):
    # try:
    global first_sleep
    for i in range(len(indices_pivot_points)):
        if (instrument_zip.get(indices_pivot_points[i]['instrument_token'])) == "BANKNIFTY":
            qty = 15
            distance = 100
            name = "BANKNIFTY"
        if (instrument_zip.get(indices_pivot_points[i]['instrument_token'])) == "NIFTY":
            qty = 50
            distance = 50
            name = "NIFTY"
        if (instrument_zip.get(indices_pivot_points[i]['instrument_token'])) == "FINNIFTY":
            qty = 40
            distance = 50
            name = "FINNIFTY"

        if datetime.time(datetime.now()) >= nine_fifteen:
            global trades_entered
            global trade_check
            tick_data = total_ticks.copy()
            if len(tick_data) > 1:
                instrument_five_minute_data = pd.DataFrame(tick_data)
                instrument_five_minute_data = instrument_five_minute_data.loc[instrument_five_minute_data['instrument_token'] == indices_pivot_points[i]['instrument_token']].set_index(
                    'datetime').loc[:, 'price'].resample('5min').ohlc().reset_index()
                instrument_five_minute_data = instrument_five_minute_data.loc[
                    instrument_five_minute_data['datetime'].dt.time >= nine_fifteen]
                support1, resistance1 = indices_pivot_points[i][
                    'support1'], indices_pivot_points[i]['resistance1']

                if len(instrument_five_minute_data) >= 2 and {instrument_five_minute_data.iloc[-2]['datetime']: name} not in trades_entered:
                    if (datetime.time(datetime.now()) == nine_twenty and instrument_five_minute_data.iloc[-2]['close'] > resistance1 and instrument_five_minute_data.iloc[-2]['open'] < resistance1) or (datetime.time(datetime.now()) == nine_twenty and instrument_five_minute_data.iloc[-2]['close'] < support1 and instrument_five_minute_data.iloc[-2]['open'] > support1):
                        instrument_close = round(
                            instrument_five_minute_data.iloc[-2]['close']/distance)*distance
                        if instrument_five_minute_data.iloc[-2]['close'] > resistance1:
                            options = puts.copy()
                            instrument_strike = instrument_close-2*distance
                        if instrument_five_minute_data.iloc[-2]['close'] < support1:
                            options = calls.copy()
                            instrument_strike = instrument_close+2*distance
                        options = options.loc[options['strike']
                                              == instrument_strike].iloc[0]
                        price = kite.ltp(options['instrument_token'])[
                            str(options['instrument_token'])]['last_price']
                        trade_check.append(dict(entry_time=datetime.now(
                        ), entry_price=price, instrument_token=options['instrument_token'], sl=price+0.2*price, exit_time=None, exit_price=None, symbol=None, side='sell', qty=qty))
                        trades_entered.append(
                            {instrument_five_minute_data.iloc[-2]['datetime']: name})
                        print('trade entered', datetime.now(),
                              indices_pivot_points[i]['instrument_token'])
                    if (len(instrument_five_minute_data) > 2 and instrument_five_minute_data.iloc[-2]['close'] > resistance1 and instrument_five_minute_data.iloc[-3]['close'] < resistance1) or (len(instrument_five_minute_data) > 2 and instrument_five_minute_data.iloc[-2]['close'] < support1 and instrument_five_minute_data.iloc[-3]['close'] > support1):
                        instrument_close = round(
                            instrument_five_minute_data.iloc[-2]['close']/100)*100
                        if instrument_five_minute_data.iloc[-2]['close'] > resistance1:
                            options = puts
                            instrument_strike = instrument_close-2*distance
                        if instrument_five_minute_data.iloc[-2]['close'] < support1:
                            options = calls
                            instrument_strike = instrument_close+2*distance
                        options = options.loc[options['strike']
                                              == instrument_strike]
                        options = options.loc[options['strike']
                                              == instrument_strike].iloc[0]
                        price = kite.ltp(options['instrument_token'])[
                            str(options['instrument_token'])]['last_price']
                        trade_check.append(dict(entry_time=datetime.now(
                        ), entry_price=price, instrument_token=options['instrument_token'], sl=price+0.2*price, exit_time=None, exit_price=None, symbol=None, side='sell', qty=qty))
                        trades_entered.append(
                            {instrument_five_minute_data.iloc[-2]['datetime']: name})
                        print('trade entered', datetime.now(),
                              indices_pivot_points[i]['instrument_token'])

    # except Exception as error:
    #     print(error)


def exits(trade_check):
    for i in range(len(trade_check)):
        if trade_check[i]['exit_time'] is None:
            data = total_ticks.copy()
            data = pd.DataFrame(data).reset_index()
            data = data.loc[(data['instrument_token'] == trade_check[i]['instrument_token']) & (
                data['datetime'] >= trade_check[i]['entry_time'])].sort_values(by='datetime')
            if len(data) > 1:
                if data.iloc[-1]['price'] > trade_check[i]['sl']:
                    trade_check[i]['exit_time'] = data.iloc[-1]['datetime']
                    trade_check[i]['exit_price'] = data.iloc[-1]['price']
                    total = pd.DataFrame(trade_check)
                    total.to_csv(r'E:\Live\pivot_points_live.csv')

                if datetime.time(data.iloc[-1]['datetime']) > three_fifteen:
                    trade_check[i]['exit_time'] = data.iloc[-1]['datetime']
                    trade_check[i]['exit_price'] = data.iloc[-1]['price']
                    total = pd.DataFrame(trade_check)
                    total.to_csv(r'E:\Live\pivot_points_live.csv')


def plot_graph(trade_check, total_ticks):
    global pnl
    if datetime.now().second == 0:
        pnl_sum = 0
        for i in range(len(trade_check)):
            print(trade_check[i])
            trade_check[i]['symbol'] = instrument_zip.get(
                trade_check[i]['instrument_token'])
            if trade_check[i]['side'] == 'sell' and trade_check[i]['exit_price'] is not None:
                pnl_sum = pnl_sum + \
                    trade_check[i]['qty']*(trade_check[i]
                                           ['entry_price']-trade_check[i]['exit_price'])
            if trade_check[i]['side'] == 'sell' and trade_check[i]['exit_price'] is None:
                tick_data = total_ticks.copy()
                tick_data = pd.DataFrame(tick_data).sort_values(by='datetime')
                tick_data = tick_data.loc[tick_data['instrument_token']
                                          == trade_check[i]['instrument_token']]
                tick_pnl = (
                    trade_check[i]['entry_price']-tick_data.iloc[-1]['price'])*trade_check[i]['qty']
                pnl_sum = pnl_sum+tick_pnl
                pnl.append(dict(time=datetime.time(
                    datetime.now()), pnl=tick_pnl))

        if len(pnl) > 2:
            plot_data = pd.DataFrame(pnl)
            plt.plot(plot_data['time'].astype('str'), plot_data['pnl'])
            plt.show()


while True:
    pivot_points_trade(total_ticks)
    exits(trade_check)
    plot_graph(trade_check, total_ticks)