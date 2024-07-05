import pandas as pd
import numpy as np 
from datetime import datetime,timedelta
from kiteconnect import KiteConnect, KiteTicker
import sqlite3
import time
from time import sleep
import pandas_ta as tec
order_updates = []
db=sqlite3.connect(r'E:/Live/market_duck_data.ddb')
c=db.cursor()
api_key = ""
api_secret = ""
access_token = ""
kws = KiteTicker(api_key, access_token)
kite = KiteConnect(api_key)
kite.set_access_token(access_token)
subscribe_list = []
master_contracts = pd.DataFrame(kite.instruments('NFO'))
banknifty = master_contracts.loc[master_contracts['name'] == 'BANKNIFTY'].sort_values(by='expiry')
banknifty = banknifty.loc[banknifty['expiry'] == banknifty.iloc[0]['expiry']]
master_contracts = pd.DataFrame(kite.instruments('NSE'))
nifty_fifty = int(master_contracts.loc[master_contracts['name'] == 'NIFTY 50'].iloc[0]['instrument_token'])
banknifty_spot = int(master_contracts.loc[master_contracts['name'] == 'NIFTY BANK'].iloc[0]['instrument_token'])
subscribe_list.append(nifty_fifty)
subscribe_list.append(banknifty_spot)
bn_ltp = kite.ltp(banknifty_spot)[str(banknifty_spot)]['last_price']
banknifty = banknifty[(banknifty['strike'] >= bn_ltp - 5000) & (banknifty['strike'] <= bn_ltp + 5000)]
token_name_dict=dict(zip(banknifty['instrument_token'],banknifty['tradingsymbol']))
call_df=banknifty.loc[banknifty['tradingsymbol'].str.contains('CE')]
put_df=banknifty.loc[banknifty['tradingsymbol'].str.contains('PE')]
subscribe_list = subscribe_list + banknifty['instrument_token'].astype('int').to_list()
nine_thirty = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
nine_fifteen = datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
three_thirty= datetime.now().replace(hour=15, minute=30, second=0, microsecond=0)
orders_placed=[]
straddle_orders_placed=[]
strangle_orders_placed=[]
straddle_placed=False
strangle_placed=False
strangle_sl_placed=False
straddle_sl_placed=False
nearest_premimum_strangle=50
premium_psar=100
psar_sl=30
psar_target=60
psar_trade_open=False
psar_sl_placed=False
psar_target_placed=False
psar_sl_id=0
psar_qty=105
psar_hedge_id=0
strangle_call_sl_id=0
strangle_put_sl_id=0
strangle_call_hedge_id=0
strangle_put_hedge_id=0


def on_order_update(ws, data):
    order_updates.append(data)





    

kws.on_order_update = on_order_update
kws.connect(threaded=True)
def place_strangle_sl():
    global strangle_orders_placed
    global strangle_placed
    pass


















def psar_place_sl():
    global orders_placed
    global trade_open
    global psar_sl_id
    global psar_hedge_id
    global psar_target_id
    global psar_sl_placed
    global psar_trade_open
    global psar_target_placed
    if psar_sl_placed ==False and psar_target_placed==False and psar_trade_open==True:
        df=pd.DataFrame(order_updates).copy(deep=True)
        for i in range(len(orders_placed)):
            completed_orders=df.loc[df['order_id']==orders_placed[i]['order_id']].sort_values(by='order_timestamp').iloc[-1] ['status']
            if completed_orders=='COMPLETE' and orders_placed[i]['name']=='psar_put_sell' :#
                psar_hedge_id=orders_placed[i]['corresponding_hedge']
                print('true')
                previous_minute=(orders_placed[i]['time_stamp']-timedelta(seconds=55)).strftime('%Y-%m-%d %H:%M:%S')
                current_minute=orders_placed[i]['time_stamp'].strftime('%Y-%m-%d %H:%M:%S')
                symbol=orders_placed[i]["token"]
                query = f"SELECT * FROM tick_data WHERE symbol={symbol} AND timestamp > '{previous_minute}' AND timestamp < '{current_minute}'"
                data = pd.read_sql_query(sql=query, con=db,parse_dates=['timestamp']).set_index('timestamp').loc[:,'price'].resample('5min').ohlc().dropna()
                data['token']=symbol
                data['name']=data['token'].astype('float').map(token_name_dict).fillna("")            
                try:
                    psar_sl_id = kite.place_order(tradingsymbol=data.iloc[0]['name'],
                                              exchange=kite.EXCHANGE_NFO,
                                              transaction_type=kite.TRANSACTION_TYPE_BUY,
                                              quantity=psar_qty,
                                              variety=kite.VARIETY_REGULAR,
                                              order_type=kite.ORDER_TYPE_SL,
                                              price=data.iloc[-1]['open']+psar_sl+10,
                                              trigger_price=data.iloc[-1]['open']+psar_sl,
                                              product=kite.PRODUCT_MIS,
                                              validity=kite.VALIDITY_DAY)
                    
    
                    psar_target_id = kite.place_order(tradingsymbol=data.iloc[0]['name'],
                                              exchange=kite.EXCHANGE_NFO,
                                              transaction_type=kite.TRANSACTION_TYPE_BUY,
                                              quantity=psar_qty,
                                              variety=kite.VARIETY_REGULAR,
                                              order_type=kite.ORDER_TYPE_LIMIT,
                                              price=max(data.iloc[-1]['open']-psar_target,0.5),
                                              
                                              product=kite.PRODUCT_MIS,
                                              validity=kite.VALIDITY_DAY)
                except Exception as error:
                    print(error)
                    
                    
                else:
                    psar_sl_id=psar_sl_id
                    psar_target_id=psar_target_id
                    psar_sl_placed=True
                    psar_target_placed=True
    
            if completed_orders=='COMPLETE' and orders_placed[i]['name']=='psar_call_sell' :#
                psar_hedge_id=orders_placed[i]['corresponding_hedge']
                print('true')
                previous_minute=(orders_placed[i]['time_stamp']-timedelta(seconds=50)).strftime('%Y-%m-%d %H:%M:%S')
                current_minute=orders_placed[i]['time_stamp'].strftime('%Y-%m-%d %H:%M:%S')
                symbol=orders_placed[i]["token"]
                query = f"SELECT * FROM tick_data WHERE symbol={symbol} AND timestamp > '{previous_minute}' AND timestamp < '{current_minute}'"
                data = pd.read_sql_query(sql=query, con=db,parse_dates=['timestamp']).set_index('timestamp').loc[:,'price'].resample('5min').ohlc().dropna()
                data['token']=symbol
                data['name']=data['token'].astype('float').map(token_name_dict).fillna("")            
                try:
                    psar_sl_id = kite.place_order(tradingsymbol=data.iloc[0]['name'],
                                              exchange=kite.EXCHANGE_NFO,
                                              transaction_type=kite.TRANSACTION_TYPE_BUY,
                                              quantity=psar_qty,
                                              variety=kite.VARIETY_REGULAR,
                                              order_type=kite.ORDER_TYPE_SL,
                                              price=data.iloc[-1]['open']+psar_sl+10,
                                              trigger_price=data.iloc[-1]['open']+psar_sl,
                                              product=kite.PRODUCT_MIS,
                                              validity=kite.VALIDITY_DAY)
                    
    
                    psar_target_id = kite.place_order(tradingsymbol=data.iloc[0]['name'],
                                              exchange=kite.EXCHANGE_NFO,
                                              transaction_type=kite.TRANSACTION_TYPE_BUY,
                                              quantity=psar_qty,
                                              variety=kite.VARIETY_REGULAR,
                                              order_type=kite.ORDER_TYPE_LIMIT,
                                              price=max(data.iloc[-1]['open']-psar_target,0.5),
                                              
                                              product=kite.PRODUCT_MIS,
                                              validity=kite.VALIDITY_DAY)
                except Exception as error:
                    print(error)
                    
                    
                else:
                    psar_sl_id=psar_sl_id
                    psar_target_id=psar_target_id
                    psar_sl_placed=True
                    psar_target_placed=True
    
    
    
def psar_trade_open_check():
    global psar_trade_open
    global order_updates
    global orders_placed
    global psar_sl_placed
    global psar_target_id
    global psar_hedge_id
    global psar_target_placed
    try:
        orders_placed_df=pd.DataFrame(orders_placed)
        order_status=pd.DataFrame(order_updates)
        sl_order_status=order_status.loc[order_status['order_id']==psar_sl_id].sort_values(by='exchange_timestamp').iloc[-1]['status']
        if sl_order_status!='TRIGGER PENDING':
            psar_trade_open=False
            psar_sl_placed=False
            psar_target_placed=False
            orders_placed=[]
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=psar_target_id)
            try:
                put_order_id = kite.place_order(tradingsymbol=psar_hedge_id,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                          quantity=psar_qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
            except:
                pass
    except:
         pass
    try:
        order_status=pd.DataFrame(order_updates)
        target_order_status=order_status.loc[order_status['order_id']==psar_target_id].sort_values(by='exchange_timestamp').iloc[-1]['status']
        if target_order_status=='COMPLETE':
            psar_trade_open=False
            psar_sl_placed=False
            psar_target_placed=False
            orders_placed=[]
            kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=psar_sl_id)
            try:
                put_order_id = kite.place_order(tradingsymbol=psar_hedge_id,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                          quantity=psar_qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
            except:
                pass

    except:
        pass

def psar(time_now):
    global psar_trade_open
    start_time=time.time()
    data = pd.read_sql(
    sql="select * from tick_data where symbol = 260105 ",
    con=db,
    parse_dates=['timestamp']).set_index('timestamp').loc[:,'price'].resample('1min').ohlc().dropna().reset_index()
    data=data.loc[(data['timestamp'].dt.time>=datetime.time(nine_fifteen)) & (data['timestamp'].dt.time<datetime.time(three_thirty)) ]
    
    psar=tec.psar(data['high'],data['low'],data['close'])
    data['psaru'],data['psarl']=psar['PSARs_0.02_0.2'].fillna(0),psar['PSARl_0.02_0.2'].fillna(0)
    end_time=time.time()
    print(data)
    if (data.iloc[-1]['psaru']==0 and data.iloc[-2]['psaru']!=0 ) or (data.iloc[-1]['psarl']==0 and data.iloc[-2]['psarl']!=0) and psar_trade_open==False and datetime.time(data.iloc[-2]['timestamp'])>datetime.time(nine_fifteen) :
        previous_minute=(data.iloc[-2]['timestamp']+timedelta(minutes=4,seconds=40)).strftime('%Y-%m-%d %H:%M:%S')
        current_minute=data.iloc[-1]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        options_data = pd.read_sql(
        sql=f"select * from tick_data where timestamp>'{previous_minute}' and timestamp<'{current_minute}'",
        con=db,
        parse_dates=['timestamp'])
        options_data['name']=options_data['symbol'].astype('float').map(token_name_dict).fillna("")
        calls=options_data.loc[options_data['name'].str.contains('CE')]
        puts=options_data.loc[options_data['name'].str.contains('PE')]
        qty=105
        if data.iloc[-1]['psaru']==0 and data.iloc[-2]['psaru']!=0 and datetime.time(data.iloc[-1]['timestamp'])>datetime.time(nine_fifteen) :
            put_strike_to_sell=puts.loc[abs(puts['price']-premium_psar).idxmin()]['name']
            put_strike_to_buy=puts.loc[abs(puts['price']-5).idxmin()]['name']
            put_token=puts.loc[abs(puts['price']-premium_psar).idxmin()]['symbol']
            try:
                put_order_id = kite.place_order(tradingsymbol=put_strike_to_sell,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                          quantity=qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
                
            
                put_hedge_id = kite.place_order(tradingsymbol=put_strike_to_buy,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_BUY,
                                          quantity=qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
            except Exception as error:
                print(error)
                end_time=time.time()
            else:
                  orders_placed.append(dict(order_id=put_order_id,name='psar_put_sell',token=put_token,time_stamp=time_now,corresponding_hedge=put_strike_to_buy
                                            ))
                 
                  psar_trade_open=True
                  end_time=time.time()
            
            
            
        if data.iloc[-1]['psarl']==0 and data.iloc[-2]['psarl']!=0 and datetime.time(data.iloc[-1]['timestamp'])>datetime.time(nine_fifteen):
            call_strike_to_sell=calls.loc[abs(calls['price']-premium_psar).idxmin()]['name']
            call_strike_to_buy=calls.loc[abs(calls['price']-5).idxmin()]['name']
            call_token=calls.loc[abs(calls['price']-premium_psar).idxmin()]['symbol']
            try:
                call_order_id = kite.place_order(tradingsymbol=call_strike_to_sell,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_SELL,
                                          quantity=qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
           
                call_hedge_id=kite.place_order(tradingsymbol=call_strike_to_buy,
                                          exchange=kite.EXCHANGE_NFO,
                                          transaction_type=kite.TRANSACTION_TYPE_BUY,
                                          quantity=qty,
                                          variety=kite.VARIETY_REGULAR,
                                          order_type=kite.ORDER_TYPE_MARKET,
                                          product=kite.PRODUCT_MIS,
                                          validity=kite.VALIDITY_DAY)
            except Exception as error:
                print(error)
                end_time=time.time()
           
           
            else:
                orders_placed.append(dict(order_id=call_order_id,name='psar_call_sell',token=call_token,time_stamp=time_now,corresponding_hedge=call_strike_to_buy))
              
                psar_trade_open=True
                end_time=time.time()
    print(start_time-end_time)
    print(data)













def straddle():
    global straddle_placed
    start_time=time.time()
    nine_thirty = datetime.now().replace(hour=9, minute=29, second=40, microsecond=0)
    nine_thirty_one = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
    nine_thirty_str = nine_thirty.strftime('%Y-%m-%d %H:%M:%S')
    nine_thirty_one_str = nine_thirty_one.strftime('%Y-%m-%d %H:%M:%S')
    data = pd.read_sql(
    sql=f"select * from tick_data where symbol = 260105 and timestamp>'{nine_thirty_str}' and timestamp<'{nine_thirty_one_str}'",
    con=db,
    parse_dates=['timestamp'])
    strike=round(data.iloc[-1]['price']/100)*100
    call_sell_strike,put_sell_strike=call_df.loc[call_df['strike']==strike].iloc[0]['tradingsymbol'],put_df.loc[put_df['strike']==strike].iloc[0]['tradingsymbol']
    call_buy_strike,put_buy_strike=call_df.loc[call_df['strike']==strike+1500].iloc[0]['tradingsymbol'],put_df.loc[put_df['strike']==strike-1500].iloc[0]['tradingsymbol']
    qty=45
    try:
        call_order_id = kite.place_order(tradingsymbol=call_buy_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_BUY,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    else:
        straddle_orders_placed.append(dict(order_id=call_order_id,name='straddle_call_hedge'))
    
    
    
    try:
        call_order_id = kite.place_order(tradingsymbol=put_buy_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_BUY,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    
    
    else:
        straddle_orders_placed.append(dict(order_id=call_order_id,name='straddle_put_hedge'))
    
    
    
    
    
    try:
        call_order_id = kite.place_order(tradingsymbol=call_sell_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_SELL,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    else:
         straddle_orders_placed.append(dict(order_id=call_order_id,name='straddle_call_sell'))
     
     
    try:
        put_order_id = kite.place_order(tradingsymbol=put_sell_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_SELL,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    else:
         straddle_orders_placed.append(dict(order_id=put_order_id,name='straddle_put_sell'))
     
     
    end_time=time.time()
    
    
    print(end_time-start_time)
    
    
    
def strangle():
    global straddle_placed
    nine_thirty = datetime.now().replace(hour=9, minute=29, second=40, microsecond=0)
    nine_thirty_one = datetime.now().replace(hour=9, minute=30, second=0, microsecond=0)
    nine_thirty_str = nine_thirty.strftime('%Y-%m-%d %H:%M:%S')
    nine_thirty_one_str = nine_thirty_one.strftime('%Y-%m-%d %H:%M:%S')
    data = pd.read_sql(
    sql=f"select * from tick_data where timestamp>'{nine_thirty_str}' and timestamp<'{nine_thirty_one_str}'",
    con=db,
    parse_dates=['timestamp'])
    data['name']=data['symbol'].astype('float').map(token_name_dict).fillna("")
    calls=data.loc[data['name'].str.contains('CE')]
    puts=data.loc[data['name'].str.contains('PE')]
    call_sell_strike,put_sell_strike=calls.loc[abs(calls['price']-nearest_premimum_strangle).idxmin()]['name'],puts.loc[abs(puts['price']-nearest_premimum_strangle).idxmin()]['name']
    call_buy_strike,put_buy_strike=calls.loc[abs(calls['price']-5).idxmin()]['name'],puts.loc[abs(puts['price']-5).idxmin()]['name']
    qty=150
    try:
        call_order_id = kite.place_order(tradingsymbol=call_buy_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_BUY,
                                  quantity=300,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:  
        print(error)
    
    else:
        strangle_orders_placed.append(dict(order_id=call_order_id,name='strangle_call_hedge'))
    
    
    try:
        call_order_id = kite.place_order(tradingsymbol=put_buy_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_BUY,
                                  quantity=300,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    
    
    
    else:
        strangle_orders_placed.append(dict(order_id=call_order_id,name='strangle_put_hedge'))
    
    
    try:
        call_order_id = kite.place_order(tradingsymbol=call_sell_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_SELL,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
    else:
        strangle_orders_placed.append(dict(order_id=call_order_id,name='strangle_call_sell'))
    try:
        put_order_id = kite.place_order(tradingsymbol=put_sell_strike,
                                  exchange=kite.EXCHANGE_NFO,
                                  transaction_type=kite.TRANSACTION_TYPE_SELL,
                                  quantity=qty,
                                  variety=kite.VARIETY_REGULAR,
                                  order_type=kite.ORDER_TYPE_MARKET,
                                  product=kite.PRODUCT_MIS,
                                  validity=kite.VALIDITY_DAY)
    
    
    except Exception as error:
        print(error)
        
    else:
        strangle_orders_placed.append(dict(order_id=put_order_id,name='strangle_put_sell'))
    end_time=time.time()
    strangle_placed=True    
    
   




