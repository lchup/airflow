import telegram
from telegram import InputMediaPhoto
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
import pandahouse as ph
from datetime import datetime
from datetime import date, timedelta


from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
}

### DAG
default_args={
            'owner': 'L.Anoshkina',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=3),
            'start_date': datetime(2024, 1, 16),
        }

    

schedule_interval = '*/15 * * * *'

dashboard = ""

chat_channel = -00000

my_token = ''
bot = telegram.Bot(token=my_token)

    #статусы is_alert:
    #1 - аномалия обнаржена
    #0 - аномалия не обнаружена

    

def interquartile_range(data, metric, a=3, n=5):
    if metric == 'ctr':
        n = n+1
    df = data.copy()
    df['q25'] = df[metric].rolling(n).quantile(0.25)
    df['q75'] = df[metric].rolling(n).quantile(0.75)
    df['igr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['igr']
    df['low'] = df['q25'] - a*df['igr']
    
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df

def sigm(data, metric, a=3, n=5):
    if metric == 'ctr':
        n = n+1
    df = data.copy()
    mean = df[metric].rolling(n).mean()
    var = df[metric].rolling(n).std()
    df['up'] = mean + a*var
    df['low'] = mean - a*var
    df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df

def create_plot(df_iqr, df_sigm, metric):
    sns.set_style("whitegrid")
    figure, axes = plt.subplots(2, 1, figsize=(20, 12))
    plt.subplots_adjust(hspace=0.6)
    sns.lineplot(ax = axes[0], data = df_iqr, x='ts', y=metric)
    sns.lineplot(ax = axes[0], data = df_iqr, x='ts', y=df_iqr['up'])
    sns.lineplot(ax = axes[0], data = df_iqr, x='ts', y=df_iqr['low'])
    axes[0].set_title(metric + " Межкварильный размах")
    
    sns.lineplot(ax = axes[1], data = df_sigm, x='ts', y=metric)
    sns.lineplot(ax = axes[1], data = df_sigm, x='ts', y=df_sigm['up'])
    sns.lineplot(ax = axes[1], data = df_sigm, x='ts', y=df_sigm['low'])
    axes[1].set_title(metric + " Правило трех сигм")
    
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plt.close()
    
    return plot_object

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anoshkina_bot_alert_dag():
    
    @task()
    def get_metrics():
        q = """
            SELECT ts, date, activeUsersFeed, activeUsersMess, views, likes, ctr, messages FROM
            (SELECT 
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    count(DISTINCT user_id) as activeUsersFeed,
                    countIf(user_id, action='view') as views,
                    countIf(user_id, action='like') as likes,
                    likes/views as ctr
             FROM simulator_20231220.feed_actions
             WHERE time >= today()-1 AND time < toStartOfFifteenMinutes(now())
             GROUP BY ts, date) t1
             JOIN
             (SELECT
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    count(DISTINCT user_id) as activeUsersMess,
                    count(user_id) as messages
             FROM simulator_20231220.message_actions
             WHERE time >= today()-1 AND time < toStartOfFifteenMinutes(now())
             GROUP BY ts, date) t2
             using ts
             ORDER BY ts
             """
        metrics = ph.read_clickhouse(q, connection=connection)
        return metrics

    @task()
    def run(data, chat=None):
        chat_id = chat or 515577853
        metrics_list = list(data.columns.drop(['ts', 'date']))
        #metrics_list = ['activeUsersFeed']

        for metric in metrics_list:
            df = data[['ts', 'date', metric]].copy()

            #проверяем по межквартильному размаху
            is_alert_iqr, df_iqr = interquartile_range(df, metric)
            #проверяем по правилу сигм
            is_alert_sigm, df_sigm = sigm(df, metric)

            if is_alert_iqr == 1 and is_alert_sigm == 1:  
                current_val = df[metric].iloc[-1]
                if metric == 'ctr':
                    current_val = round(current_val, 2)
                last_val = round(100-df[metric].iloc[-1]*100/df[metric].iloc[-2], 2)
                msg = "<b>Обнаружена аномалия!</b>\nМетрика: <b>" + str(metric) + \
                "</b> \nТекущее значение: " + str(current_val) + \
                "\nОтклонение от предыдущего значения: " + str(last_val) + "%\n" + str(dashboard)
                plot = create_plot(df_iqr, df_sigm, metric)
                bot.sendMessage(chat_id=chat_id, text=msg, parse_mode='html')
                bot.sendPhoto(chat_id=chat_id, photo=plot)


    metrics = get_metrics()
    run(metrics, chat_channel)
    
anoshkina_bot_alert_dag = anoshkina_bot_alert_dag()