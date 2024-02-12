import telegram
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


chat_channel = -000000000
my_token = ''
bot = telegram.Bot(token=my_token)

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

    
#schedule_interval = timedelta(days=1)
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anoshkina_bot_dag():
    
    @task()
    def get_metrics():
        q = """
            SELECT 
                toDate(time) as Date,
                count(DISTINCT user_id) as DAU,
                sum(action = 'like') as Likes,
                sum(action = 'view') as Views,
                Likes/Views as CTR
            FROM simulator_20231220.feed_actions
            WHERE toDate(time) BETWEEN today()-7 AND yesterday()
            GROUP BY toDate(time)        
        """
        metrics = ph.read_clickhouse(q, connection=connection)
        return metrics
    
    @task()
    def get_message(metrics):
        message = "<b>Отчет по ленте новостей \n</b>"
        yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        metrics_yest = metrics[(metrics['Date']==yesterday)]
        
        message = message + "<b>Ключевые метрики: " + str(yesterday) + "</b>\n" + \
                      "DAU: " + str(metrics_yest['DAU'].values[0]) + "\n" + \
                      "Лайки: " + str(metrics_yest['Likes'].values[0]) + "\n" + \
                      "Просмотры: " + str(metrics_yest['Views'].values[0]) + "\n" + \
                      "CTR: " + str(round(metrics_yest['CTR'].values[0], 3))
        
        return message
    
    @task()
    def send_info(metrics, message, chat=None):
        chat_id = chat or 515577853
        
        m_name = list(metrics.columns.drop(['Date']))
        n = len(m_name)
        
        sns.set_style("whitegrid")
        figure, axes = plt.subplots(n, 1, figsize=(10, 12))
        plt.subplots_adjust(hspace=0.6)
        figure.suptitle('Динамика ключевых показателей за последнюю неделю')
        
        for i in range(n):
            axes[i].set_title(m_name[i], size=15)
            sns.lineplot(ax=axes[i], data=metrics, x='Date', y=m_name[i], marker= '.', markersize=10)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plt.close()
        
        bot.sendMessage(chat_id=chat_id, text=message, parse_mode='html')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    metrics = get_metrics()
    message = get_message(metrics)
    send_info(metrics, message, chat_channel)
    
anoshkina_bot_dag = anoshkina_bot_dag()

    
    

