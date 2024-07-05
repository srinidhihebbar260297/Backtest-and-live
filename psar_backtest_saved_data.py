import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pandas_ta as tec
import pdb
import matplotlib.pyplot as plt
from math import floor
import random
upper_triggers=[]
lower_triggers=[]
total=[]
sl_points=50
first_book_qty=105
target_points=50
closest_premium=100
other_side_premium=50

nine_fifteen=datetime.time(datetime.now().replace(hour=9,minute=15,second=0,microsecond=0))
three=datetime.time(datetime.now().replace(hour=15,minute=25,second=0,microsecond=0))
two=datetime.time(datetime.now().replace(hour=15,minute=0,second=0,microsecond=0))
last_side=None
expiries=pd.DataFrame(pd.date_range(start='31/12/2016',end='31/12/2024',))
expiries=expiries.loc[expiries[0].dt.weekday==3]
expiries.columns=['expiry']
data=pd.read_csv(r'D:/BANKNIFTY/nifty50.csv',parse_dates=['date'])
# data=data.loc[data['date'].dt.year>=2023]
data=data.loc[data['date'].dt.year>=2023]
data=data.loc[data['date'].dt.month<=6]
ohlc={'open':'first','high':'max','low':'min','close':'last'}


five_data=data.set_index('date').resample('1min').apply(ohlc).dropna().reset_index()
psar=tec.psar(high=five_data['high'],low= five_data['low'],af0=0.02 )
five_data['psaru'],five_data['psarl']=psar['PSARs_0.02_0.2'].fillna(0),psar['PSARl_0.02_0.2'].fillna(0)    

df=five_data.to_dict('records')

for i in range(len(df[1:])):
    if df[i]['psaru']!=0 and df[i-1]['psaru']==0  and datetime.time(df[i]['date'])>nine_fifteen and datetime.time(df[i-1]['date'])<two :
        lower_triggers.append(df[i+1])
        
    if df[i]['psarl']!=0 and df[i-1]['psarl']==0 and datetime.time(df[i]['date'])>nine_fifteen  and datetime.time(df[i-1]['date'])<two  :
        upper_triggers.append(df[i+1])
        
upper_triggers=pd.DataFrame(upper_triggers)
lower_triggers=pd.DataFrame(lower_triggers)


upper_triggers['side']='PUT'
lower_triggers['side']='CALL'
triggers=pd.concat([upper_triggers,lower_triggers]).sort_values(by='date')
triggers=triggers.to_dict('records')
status={'date':None,'trigger_time':None,'entry_time':None,'entry_price':None,
        'entry_side':None,'sl':None,'qty':None,'trailed':None,'exit_time':None,
        'exit_price':None,'pnl':None}
total.append(dict(exit_time=datetime.now()-timedelta(5000),entry_side=None))
capital=1200000
entered=[]

for i in range(len(triggers)):
    status={'date':None,'trigger_time':None,'entry_time':None,'entry_price':None,
        'entry_side':None,'sl':None,'qty':None,'trailed':None,'exit_time':None,
        'exit_price':None,'pnl':None,'other_side_exit_price':None}

    try:
        if triggers[i]['date'].replace(tzinfo=None)>total[-1]['exit_time'].replace(tzinfo=None):# and entered.count(datetime.date(triggers[i]['date']))<2:
            entered.append(datetime.date(triggers[i]['date']))
            from_date=datetime.date(triggers[i]['date'])
            expiry_date=datetime.date(expiries.loc[expiries['expiry'].dt.date>=from_date].iloc[0][0])
            if expiry_date==datetime.date(datetime.now().replace(year=2023,month=1,day=26)): expiry_date=expiry_date.replace(day=25)
            if expiry_date==datetime.date(datetime.now().replace(year=2023,month=3,day=30)): expiry_date=expiry_date.replace(day=29)
            if expiry_date==datetime.date(datetime.now().replace(year=2018,month=3,day=29)): expiry_date=expiry_date.replace(day=28)
            if expiry_date==datetime.date(datetime.now().replace(year=2018,month=9,day=13)): expiry_date=expiry_date.replace(day=12)
            if expiry_date==datetime.date(datetime.now().replace(year=2018,month=9,day=20)): expiry_date=expiry_date.replace(day=19)
            if expiry_date==datetime.date(datetime.now().replace(year=2018,month=10,day=18)): expiry_date=expiry_date.replace(day=17)
            if expiry_date==datetime.date(datetime.now().replace(year=2018,month=11,day=8)): expiry_date=expiry_date.replace(day=7)
            if expiry_date==datetime.date(datetime.now().replace(year=2019,month=3,day=21)): expiry_date=expiry_date.replace(day=20)
            if expiry_date==datetime.date(datetime.now().replace(year=2019,month=8,day=15)): expiry_date=expiry_date.replace(day=14)
            if expiry_date==datetime.date(datetime.now().replace(year=2020,month=4,day=2)): expiry_date=expiry_date.replace(day=1)
            if expiry_date==datetime.date(datetime.now().replace(year=2021,month=3,day=11)): expiry_date=expiry_date.replace(day=10)
            if expiry_date==datetime.date(datetime.now().replace(year=2021,month=5,day=13)): expiry_date=expiry_date.replace(day=12)
            if expiry_date==datetime.date(datetime.now().replace(year=2021,month=8,day=19)): expiry_date=expiry_date.replace(day=18)
            if expiry_date==datetime.date(datetime.now().replace(year=2021,month=11,day=4)): expiry_date=expiry_date.replace(day=3)
            if expiry_date==datetime.date(datetime.now().replace(year=2022,month=4,day=14)): expiry_date=expiry_date.replace(day=13)
            if datetime.date(triggers[i]['date'])==datetime.date(datetime.now().replace(year=2023,month=6,day=28)) : expiry_date=expiry_date.replace(day=28)
            date,expiration=from_date.strftime('%d%m%Y'),expiry_date.strftime('%d%m%Y')
            if triggers[i]['side']=='PUT':
                option_side='PE'
                
            if triggers[i]['side']=='CALL':
                option_side='CE'
            call_data=f'D:/NIFTY/NIFTY/NIFTY_CE_{date}_{expiration}.csv'
            put_data=f'D:/NIFTY/NIFTY/NIFTY_PE_{date}_{expiration}.csv'
            call_data=pd.read_csv(call_data,parse_dates=[['date','time']])
            put_data=pd.read_csv(put_data,parse_dates=[['date','time']])
            
            if triggers[i]['side']=='CALL':
                
                obs_call=call_data.loc[call_data['date_time'].dt.time==datetime.time(triggers[i]['date'])]
                obs_put=put_data.loc[put_data['date_time'].dt.time==datetime.time(triggers[i]['date'])]
                call_strike_to_sell=obs_call.loc[abs(obs_call['open']-closest_premium).idxmin()]['strike_price']
                # call_strike_to_sell=round(triggers[i]['open']/100)*100
                put_strike_to_sell=obs_put.loc[abs(obs_put['open']-other_side_premium).idxmin()]['strike_price']
                call_data=call_data.loc[call_data['strike_price']==call_strike_to_sell]
                put_data=put_data.loc[put_data['strike_price']==put_strike_to_sell]
                call_data=call_data.loc[call_data['date_time'].dt.time>=datetime.time(triggers[i]['date'])]
                put_data=put_data.loc[put_data['date_time'].dt.time>=datetime.time(triggers[i]['date'])]
                options_data=call_data
                other_side_data=put_data
                options_data=options_data.merge(other_side_data,on='date_time',how='inner')
                options_data['open']=options_data['open_x']+options_data['open_y']
                options_data['high']=options_data['high_x']+options_data['high_y']
                options_data['low']=options_data['low_x']+options_data['low_y']
                options_data['close']=options_data['close_x']+options_data['close_y']
                
            if triggers[i]['side']=='PUT':
                obs_call=call_data.loc[call_data['date_time'].dt.time==datetime.time(triggers[i]['date'])]
                obs_put=put_data.loc[put_data['date_time'].dt.time==datetime.time(triggers[i]['date'])]
                call_strike_to_sell=obs_call.loc[abs(obs_call['open']-other_side_premium).idxmin()]['strike_price']
                
                put_strike_to_sell=obs_put.loc[abs(obs_put['open']-closest_premium).idxmin()]['strike_price']
                # put_strike_to_sell=round(triggers[i]['open']/100)*100
                call_data=call_data.loc[call_data['strike_price']==call_strike_to_sell]
                put_data=put_data.loc[put_data['strike_price']==put_strike_to_sell]
                call_data=call_data.loc[call_data['date_time'].dt.time>=datetime.time(triggers[i]['date'])]
                put_data=put_data.loc[put_data['date_time'].dt.time>=datetime.time(triggers[i]['date'])]
                options_data=put_data
                other_side_data=call_data
                options_data=options_data.merge(other_side_data,on='date_time',how='inner')
                options_data['open']=options_data['open_x']+options_data['open_y']
                options_data['high']=options_data['high_x']+options_data['high_y']
                options_data['low']=options_data['low_x']+options_data['low_y']
                options_data['close']=options_data['close_x']+options_data['close_y']
            options_data['date']=options_data['date_time']
            options_data=options_data.to_dict('records')
            other_side_data=other_side_data.to_dict('records')
            status['entry_price']=random.uniform(options_data[0]['high'],options_data[0]['low'])
            status['entry_time']=datetime.time(options_data[0]['date'])
            status['entry_side']=option_side
            status['trigger_time']=datetime.time(options_data[0]['date']-timedelta(minutes=5))
            status['date']=datetime.date(options_data[0]['date'])
            status['sl']=status['entry_price']+sl_points
            status['target']=max(status['entry_price']-target_points,0)
            status['qty']=floor(capital/80000)*15
            status['qty']=500
            status['capital']=capital-700
            status['pnl']=0
            status['call_price']=options_data[0]['open_x']
            status['put_price']=options_data[0]['open_y']
            tbb=pd.DataFrame(total)
            # status['capital']=320000
            # status['qty']=min(   (round(round(capital*0.02/sl_points)/15))*15       ,(round((floor(status['capital']/2000))/15)*15))
            # first_book_qty=round((round(status['qty']/2))/15)*15
            # pdb.set_trace()
            for j in range(len(options_data)):
                if status['date'] is not None and status['exit_time'] is None and options_data[j]['low']<status['target']:
                    status['exit_time']=options_data[j]['date']
                    status['exit_price']=status['target']+2
                    status['call_ext_price']=options_data[j]['low_x']
                    status['put_ext_price']=options_data[j]['low_y']
                    status['pnl']=status['qty']*(status['entry_price']-status['exit_price'])+status['pnl']
                    capital=capital+status['pnl']
                    total.append(status)
                    status={'date':None,'trigger_time':None,'entry_time':None,'entry_price':None,
                            'entry_side':None,'sl':None,'qty':None,'trailed':None,'exit_time':None,
                            'exit_price':None,'pnl':None}
                if status['date'] is not None and status['exit_time'] is None and options_data[j]['high']>status['sl']:
                    status['exit_time']=options_data[j]['date']
                    status['exit_price']=options_data[j]['high']
                    status['call_ext_price']=options_data[j]['high_x']
                    status['put_ext_price']=options_data[j]['high_y']
                    status['pnl']=status['qty']*(status['entry_price']-status['exit_price'])+status['pnl']
                    capital=capital+status['pnl']
                    total.append(status)
                    status={'date':None,'trigger_time':None,'entry_time':None,'entry_price':None,
                            'entry_side':None,'sl':None,'qty':None,'trailed':None,'exit_time':None,
                            'exit_price':None,'pnl':None}
                if status['date'] is not None and status['exit_time'] is None and datetime.time(options_data[j]['date'])>three:
                    status['exit_time']=options_data[j]['date']
                    status['exit_price']=options_data[j]['close']+2
                    status['call_ext_price']=options_data[j]['close_x']
                    status['put_ext_price']=options_data[j]['close_y']
                    status['pnl']=status['qty']*(status['entry_price']-status['exit_price'])+status['pnl']
                    capital=capital+status['pnl']
                    total.append(status)
                    status={'date':None,'trigger_time':None,'entry_time':None,'entry_price':None,
                            'entry_side':None,'sl':None,'qty':None,'trailed':None,'exit_time':None,
                            'exit_price':None,'pnl':None}
                    
                # if status['date'] is not None and status['exit_time'] is None and status['trailed'] is None and options_data[j]['low']<=status['entry_price']-15:
                #     status['trailed']=status['sl']
                #     status['sl']=status['entry_price']-2
                #     # status['pnl']=(status['qty']-first_book_qty)*(sl_points-2)
                #     # status['first_book_qty']=first_book_qty
                #     # status['qty']=status['qty']-first_book_qty
                    

    except Exception as error:
        print(error)
        








total=pd.DataFrame(total)
total['pnl']=total['pnl']
# total['pnl']=total['entry_price']-total['exit_price']
print(total['pnl'].sum())


def calculate_and_plot_cumulative_returns_and_drawdown(trade_data, date_column, pnl_column):
    # Ensure the 'date' column is in datetime format
    trade_data[date_column] = pd.to_datetime(trade_data[date_column])

    # Calculate cumulative returns
    trade_data['cumulative_returns'] = trade_data[pnl_column].cumsum()

    # Calculate drawdown
    trade_data['cumulative_high'] = trade_data['cumulative_returns'].cummax()
    trade_data['drawdown'] = trade_data['cumulative_returns'] - trade_data['cumulative_high']

    # Calculate max drawdown
    max_drawdown = trade_data['drawdown'].min()

    # Create two separate subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # Plot Cumulative Returns
    ax1.plot(trade_data[date_column], trade_data['cumulative_returns'], marker='o', linestyle='-', color='dodgerblue', label='Cumulative Returns', linewidth=2)
    ax1.set_ylabel('Value', fontsize=12)
    ax1.set_title('Cumulative Returns from Individual Trades', fontsize=14)
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.legend(loc='upper left', fontsize=12)

    # Plot Drawdown
    ax2.plot(trade_data[date_column], trade_data['drawdown'], marker='x', linestyle='--', color='tomato', label='Drawdown', linewidth=2)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.set_ylabel('Drawdown', fontsize=12)
    ax2.set_title('Drawdown from Individual Trades', fontsize=14)
    ax2.grid(True, linestyle='--', alpha=0.7)
    ax2.legend(loc='upper left', fontsize=12)

    # Display max drawdown as text on the Drawdown chart
    ax2.text(0.05, 0.9, f'Max Drawdown: {max_drawdown:.2%}', transform=ax2.transAxes, fontsize=12, color='red')

    # Rotate x-axis labels for better readability (adjust as needed)
    ax1.tick_params(axis='x', rotation=45)
    ax2.tick_params(axis='x', rotation=45)
    # Show the plots
    plt.tight_layout()
    plt.show()



calculate_and_plot_cumulative_returns_and_drawdown(total, 'date', 'pnl')





total.to_csv(r'E:/python_backtest/twitter-2023sept.csv')





per_day=total.set_index('exit_time').loc[:,'pnl'].resample('1d').apply({'pnl':'sum'}).dropna()

































