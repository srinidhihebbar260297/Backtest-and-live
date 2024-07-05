import pandas as pd
import numpy as np
from datetime import datetime, timedelta, timezone, time
import pandas_ta as tec
import maticalgos.historical as historical
import datetime as ddt
import matplotlib.pyplot as plt
mail_id = "srinidhi260297@gmail.com" #Enter your mail here
password_recieved = "645543" #Enter your passowrd here
instrument_name = "banknifty" # Enter instrument name as nifty or banknifty as per your requirement in lower case
from_date = ddt.date(2024, 1, 1)
to_date = ddt.date(2024, 1, 10) # Enter date in format YY/MM/DD - Data available till 2023 November 30th when checked on 28/01/2024
ma= historical(mail_id)
ma.login(password_recieved) 
df = pd.DataFrame()
date_check = from_date
positions=[]
trade_open=False
total=[]
nine_thirty = time(9, 30,0)
three=time(15, 15,0)
two=time(15, 0,0)
bank_nifty_spot=pd.read_csv(r'D:/BANKNIFTY/banknifty.csv',parse_dates=['date'])
bank_nifty_spot=bank_nifty_spot.loc[bank_nifty_spot['date'].dt.year>=2019].copy()
bank_nifty_spot_day_data=bank_nifty_spot.set_index('date').resample('1d',offset='9h15m').apply({'open':'first','high':'max','low':'min','close':'last'}).reset_index().dropna()
dates=bank_nifty_spot_day_data['date'].unique()

status={'date':None,'trigger_time':None,'entry_time':None,'entry_side':None,'entry_symbol':None,'entry_price':None,'entry_sl':None,'entry_hedge':None,'exit_time':None,'exit_price':None}

total.append({'exit_time':datetime.now()-timedelta(10000)})
def calculate_pivot_points(high, low, close):
    pivot = (high + low + close) / 3

    support1 = (2 * pivot) - high
    support2 = pivot - (high - low)
    support3 = low - 2 * (high - pivot)

    resistance1 = (2 * pivot) - low
    resistance2 = pivot + (high - low)
    resistance3 = high + 2 * (pivot - low)

    return {
        'pivot': pivot,
        'support1': support1,
        'support2': support2,
        'support3': support3,
        'resistance1': resistance1,
        'resistance2': resistance2,
        'resistance3': resistance3
    }


def calculate_and_plot_cumulative_returns_and_drawdown(trade_data, date_column, pnl_column):

    trade_data[date_column] = pd.to_datetime(trade_data[date_column])


    trade_data['cumulative_returns'] = trade_data[pnl_column].cumsum()

    trade_data['cumulative_high'] = trade_data['cumulative_returns'].cummax()
    trade_data['drawdown'] = trade_data['cumulative_returns'] - trade_data['cumulative_high']

    max_drawdown = trade_data['drawdown'].min()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)


    ax1.plot(trade_data[date_column], trade_data['cumulative_returns'], marker='o', linestyle='-', color='dodgerblue', label='Cumulative Returns', linewidth=2)
    ax1.set_ylabel('Value', fontsize=12)
    ax1.set_title('Cumulative Returns from Individual Trades', fontsize=14)
    ax1.grid(True, linestyle='--', alpha=0.7)
    ax1.legend(loc='upper left', fontsize=12)


    ax2.plot(trade_data[date_column], trade_data['drawdown'], marker='x', linestyle='--', color='tomato', label='Drawdown', linewidth=2)
    ax2.set_xlabel('Date', fontsize=12)
    ax2.set_ylabel('Drawdown', fontsize=12)
    ax2.set_title('Drawdown from Individual Trades', fontsize=14)
    ax2.grid(True, linestyle='--', alpha=0.7)
    ax2.legend(loc='upper left', fontsize=12)


    ax2.text(0.05, 0.9, f'Max Drawdown: {max_drawdown:.2%}', transform=ax2.transAxes, fontsize=12, color='red')

    ax1.tick_params(axis='x', rotation=45)
    ax2.tick_params(axis='x', rotation=45)

    # Show the plots
    plt.tight_layout()
    plt.show()








for i in range(len(dates[1:])):
    # try:
       actual_data=bank_nifty_spot.loc[bank_nifty_spot['date'].dt.date==datetime.date(dates[i+1])] 
       pivot_check=bank_nifty_spot_day_data.loc[bank_nifty_spot_day_data['date'].dt.date==datetime.date(dates[i])].copy()
       pivot_points=calculate_pivot_points(pivot_check['high'], pivot_check['low'], pivot_check['close'])
       pivot_points=pd.DataFrame(pivot_points).fillna(0)
       data = ma.get_data("banknifty", datetime.date(dates[i+1])) 
       data['datetime']=pd.to_datetime(data['date']+data['time'],format='%Y-%m-%d%H:%M:%S')
       data['open'],data['high'],data['low'],data['close']=data['open'].astype('float'),data['high'].astype('float'),data['low'].astype('float'),data['close'].astype('float')
       df = data.copy()
       spot=df.loc[df['symbol']=='BANKNIFTY'].copy()
       spot['round_strike']=round(spot['open']/100)*100
       calls=df.loc[df['symbol'].str.contains("CE")].copy()
       puts=df.loc[df['symbol'].str.contains("PE")].copy()
       calls['strike']=calls['symbol'].str[-7:-2].astype('int')
       calls['expiry']=pd.to_datetime((calls['symbol'].str[-14:-7]+"15:30:00"),format='%d%b%y%H:%M:%S')
       puts['strike']=puts['symbol'].str[-7:-2].astype(int)
       puts['expiry']= pd.to_datetime((puts['symbol'].str[-14:-7]+"15:30:00"),format='%d%b%y%H:%M:%S')
       # print(pivot_points)
       if pivot_points.iloc[0]['resistance1'] ==0 or pivot_points.iloc[0]['support1'] ==0 :
           break
       if pivot_points.iloc[0]['resistance1'] !=0 or pivot_points.iloc[0]['support1'] !=0 :
           # print('yes')
           actual_data=actual_data.set_index('date').resample('5min',offset='9h15m').apply({'open':'first','high':'max','low':'min','close':'last'}).reset_index().dropna()
           actual_data=actual_data.to_dict('records')
           for j in range(len(actual_data[1:])):
               # print('yes')
               # print(abs(pivot_points.iloc[0]['resistance1']-actual_data[j]['close']))
               # print(abs(pivot_points.iloc[0]['resistance1']-actual_data[j]['close']))
               if actual_data[j]['close']>pivot_points.iloc[0]['resistance1'] and actual_data[j-1]['close']<pivot_points.iloc[0]['resistance1'] and datetime.time(actual_data[j]['date'])<two and actual_data[j]['datetime']>total[-1]['exit_time']:
                   status['date']=datetime.date(actual_data[j]['date'])
                   status['trigger_time']=datetime.time(actual_data[j]['date'])
                   status['entry_time']=(actual_data[j]['date'])+timedelta(minutes=1)
                   strike=(round(actual_data[j]['close']/100)*100)-200
                   status['support1']=pivot_points.iloc[0]['support1']
                   status['resistance1']=pivot_points.iloc[0]['resistance1']
                   status['entry_side']='put'
                   options_data=puts.loc[(puts['strike']==strike) &(puts['datetime'].dt.time>datetime.time(actual_data[j]['date']))]
                   status['entry_symbol']=options_data.iloc[0]['symbol']
                   status['entry_price']=options_data.iloc[0]['open']
                   status['spot_previous_close']=actual_data[j]['close']
                   status['sl']=status['entry_price']+(0.2*status['entry_price'])
                   exit_data=options_data.loc[options_data['datetime'].dt.time>datetime.time(status['entry_time'])].to_dict('records')
                   for k in range(len(exit_data)):
                     
                            if status['date'] is not None and datetime.time(exit_data[k]['datetime'])>three and status['exit_price'] is None and status['date'] is not None:
                                status['exit_price']=exit_data[k]['close']
                                status['exit_time']=exit_data[k]['datetime']
                                total.append(status)
                                status={'date':None,'trigger_time':None,'entry_time':None,'entry_side':None,'entry_symbol':None,'entry_price':None,'entry_sl':None,'entry_hedge':None,'exit_time':None,'exit_price':None}
                             
                             
                            if status['date'] is not None and exit_data[k]['high']>status['sl'] and status['exit_price'] is None and status['date'] is not None:
                                status['exit_price']=status['sl']
                                status['exit_time']=exit_data[k]['datetime']
                                total.append(status)
                                status={'date':None,'trigger_time':None,'entry_time':None,'entry_side':None,'entry_symbol':None,'entry_price':None,'entry_sl':None,'entry_hedge':None,'exit_time':None,'exit_price':None}
    
               if actual_data[j]['close']<pivot_points.iloc[0]['support1'] and actual_data[j-1]['close']>pivot_points.iloc[0]['support1']  and datetime.time(actual_data[j]['date'])<two and actual_data[j]['datetime']>total[-1]['exit_time']:
                   status['date']=datetime.date(actual_data[j]['date'])
                   status['trigger_time']=datetime.time(actual_data[j]['date'])
                   status['entry_time']=(actual_data[j]['date'])+timedelta(minutes=1)
                   status['support1']=pivot_points.iloc[0]['support1']
                   status['resistance1']=pivot_points.iloc[0]['resistance1']
                   status['entry_side']='call'
                   strike=(round(actual_data[j]['close']/100)*100)+200
                   options_data=calls.loc[(calls['strike']==strike) &(calls['datetime'].dt.time>datetime.time(actual_data[j]['date']))]
                   status['entry_symbol']=options_data.iloc[0]['symbol']
                   status['entry_price']=options_data.iloc[0]['open']
                   status['spot_previous_close']=actual_data[j]['close']
                   status['sl']=status['entry_price']+(0.2*status['entry_price'])
                   exit_data=options_data.loc[options_data['datetime'].dt.time>datetime.time(status['entry_time'])].to_dict('records')
                   for k in range(len(exit_data)):
                      
                            if status['date'] is not None and  exit_data[k]['high']>status['sl'] and status['exit_price'] is None and status['date'] is not None:
                                status['exit_price']=status['sl']
                                status['exit_time']=exit_data[k]['datetime']
                                total.append(status)
                                status={'date':None,'trigger_time':None,'entry_time':None,'entry_side':None,'entry_symbol':None,'entry_price':None,'entry_sl':None,'entry_hedge':None,'exit_time':None,'exit_price':None}
                                 
                            
                            if  status['date'] is not None and datetime.time(exit_data[k]['datetime']) >three and status['exit_price'] is None and status['date'] is not None:
                                status['exit_price']=exit_data[k]['close']
                                status['exit_time']=exit_data[k]['datetime']
                                total.append(status)
                                status={'date':None,'trigger_time':None,'entry_time':None,'entry_side':None,'entry_symbol':None,'entry_price':None,'entry_sl':None,'entry_hedge':None,'exit_time':None,'exit_price':None}
                                 
                            
    
    
    






    # except Exception as error:
    #     print(error)




total=pd.DataFrame(total)
total['pnl']=(total['entry_price']-total['exit_price'])*15*10

calculate_and_plot_cumulative_returns_and_drawdown(total, 'date', 'pnl')





daily_returns = total['pnl']

sharpe_ratio = np.sqrt(252) * daily_returns.mean() / daily_returns.std()


print("Sharpe Ratio:", sharpe_ratio)




daily_returns = total['pnl']


downside_returns = daily_returns[daily_returns < 0]
downside_deviation = np.sqrt((downside_returns**2).mean())


sortino_ratio = np.sqrt(252) * daily_returns.mean() / downside_deviation
print("Sortino Ratio:", sortino_ratio)









