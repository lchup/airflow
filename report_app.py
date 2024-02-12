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

    
#schedule_interval = timedelta(days=1)
schedule_interval = '0 11 * * *'



chat_channel = -000000
my_token = ''
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anoshkina_bot_dag_1():
    
    @task()
    def get_metrics():
            q = """
                SELECT Date, Likes, Views, CTR, Messages, uniq_posts

               FROM
                 (SELECT 
                         sum(action = 'like') as Likes,
                         sum(action = 'view') as Views,
                         Likes/Views as CTR,
                         count(DISTINCT f.post_id) as uniq_posts,
                         toDate(f.time) as Date
                  FROM simulator_20231220.feed_actions f
                  WHERE  toDate(time) BETWEEN today()-8 AND yesterday()
                  GROUP  BY toDate(f.time)) t1

                INNER JOIN
                 (SELECT 
                         count(m.user_id) as Messages,
                         toDate(m.time) as Date
                  FROM simulator_20231220.message_actions m 
                  WHERE toDate(time) BETWEEN today()-8 AND yesterday()
                  GROUP  BY toDate(m.time)) t2
                  ON (t1.Date=t2.Date)
          """
            metrics = ph.read_clickhouse(q, connection=connection)
            return metrics
    
    @task()
    def get_dau():
        q = """
        SELECT count(DISTINCT user_id) as DAU, Date FROM 

            (SELECT user_id, toDate(time) as Date FROM simulator_20231220.feed_actions f
            WHERE  toDate(time) BETWEEN today()-8 AND yesterday()
            UNION ALL
            SELECT user_id, toDate(time) as Date FROM simulator_20231220.message_actions f
            WHERE  toDate(time) BETWEEN today()-8 AND yesterday()
            )
            GROUP BY Date
            """
        dau = ph.read_clickhouse(q, connection=connection)
        return dau

    @task()
    def get_users_m_l():
        q = """
        SELECT count(DISTINCT user_id) as count_m
           FROM simulator_20231220.message_actions m
           WHERE toDate(time)= yesterday() AND m.user_id NOT IN
               (SELECT f.user_id
                FROM simulator_20231220.feed_actions f
                WHERE toDate(time)=yesterday())
            GROUP by toDate(time)
            """
        only_m_users = ph.read_clickhouse(q, connection=connection)

        q = """
        SELECT count(DISTINCT user_id) as count_l
           FROM simulator_20231220.feed_actions f
           WHERE toDate(time)= yesterday() AND f.user_id NOT IN
               (SELECT m.user_id
                FROM simulator_20231220.message_actions m
                WHERE toDate(time)=yesterday())
            GROUP by toDate(time)
        """
        only_l_users = ph.read_clickhouse(q, connection=connection)

        metrics = only_m_users.merge(only_l_users, left_index=True,right_index=True)
        return metrics


    @task()
    def get_new_users():
        q = """
        SELECT start_date, count (DISTINCT user_id) as count, source
       FROM
         (SELECT user_id,
                    source,
                 min(toDate(time)) as start_date
          FROM simulator_20231220.feed_actions
          GROUP BY user_id, source) 

       WHERE start_date BETWEEN today()-8 AND yesterday()
       GROUP BY start_date, source
       ORDER BY start_date
       """
        metrics = ph.read_clickhouse(q, connection=connection)
        return metrics

    
    @task()
    def create_message(metrics, new_users, dau_df, m_l_users, chat=None):
            chat_id = chat or 515577853

            yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
            d_1 = (date.today() - timedelta(days=2)).strftime('%Y-%m-%d')
            d_8 = (date.today() - timedelta(days=8)).strftime('%Y-%m-%d')

            header = "<b>Отчет по приложению за " + str(yesterday) + "</b> "
            h2_1 = "\n\n<b>ПОЛЬЗОВАТЕЛИ</b> \n\n"

            metrics_yest = metrics[(metrics['Date']==yesterday)]
            metrics_yest_1 = metrics[(metrics['Date']==d_1)]
            metrics_yest_8 = metrics[(metrics['Date']==d_8)]


            #------------------------------------------------- DAU

            dau = dau_df[(dau_df['Date']==yesterday)]['DAU'].values[0]
            dau_1 = dau_df[(dau_df['Date']==d_1)]['DAU'].values[0]
            dau_8 = dau_df[(dau_df['Date']==d_8)]['DAU'].values[0]

            p_1 = round(dau * 100 / dau_1 - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(dau * 100 / dau_8 - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_dau = "<b>DAU по ленте новостей и мессенджеру: </b>" + \
                            str(dau) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от 7 дней назад"




            #-----------------------------------------------Only Lenta, Messenger
            only_m = m_l_users['count_m'].values[0]
            only_l = m_l_users['count_l'].values[0]
            ml = dau - only_m - only_l

            m_only = "\nПользовались <b>только лентой новостей</b>: " + str(only_l) + \
                        " человек. \nПользовались <b>только мессенджером</b>: " + str(only_m) + \
                        " человек. \nПользовались <b>и лентой и мессенджером</b>: " + str(ml) + " человек."


            #------------------------------------------------new users        
            count_new_users = new_users[(new_users['start_date']==yesterday)]['count'].sum()
            count_new_users_1 = new_users[(new_users['start_date']==d_1)]['count'].sum()
            count_new_users_8 = new_users[(new_users['start_date']==d_8)]['count'].sum()

            p_1 = round(count_new_users * 100 / count_new_users_1 -100,2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(count_new_users * 100 / count_new_users_8 -100,2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)


            m_new_users = "<b>\n\nНовые пользователи: </b>" + str(count_new_users) + "\n" + \
                        str(p_1) + "% от 1 день назад \n" + \
                        str(p_8) + "% от 7 дней назад \n"


            #-------ads
            users_ads = new_users[(new_users['start_date']==yesterday) & (new_users['source']=='ads')]['count'].values[0]
            users_ads_1 = new_users[(new_users['start_date']==d_1) & (new_users['source']=='ads')]['count'].values[0]
            users_ads_8 = new_users[(new_users['start_date']==d_8) & (new_users['source']=='ads')]['count'].values[0]

            p_1 = round(users_ads * 100 / users_ads_1 - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(users_ads * 100 / users_ads_8 - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_new_users = m_new_users + \
                        "Из них: \n<b>С рекламного трафика</b>: " + str(users_ads) + " человек. \n" +\
                            str(p_1) + "% от 1 день назад \n" + \
                            str(p_8) + "% от 7 дней назад \n"



            #-------organic
            users_org = new_users[(new_users['start_date']==yesterday) & (new_users['source']=='organic')]['count'].values[0]
            users_org_1 = new_users[(new_users['start_date']==d_1) & (new_users['source']=='organic')]['count'].values[0]
            users_org_8 = new_users[(new_users['start_date']==d_8) & (new_users['source']=='organic')]['count'].values[0]

            p_1 = round(users_org * 100 / users_org_1 - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(users_org * 100 / users_org_8 - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_new_users = m_new_users + \
                        "<b>C органического трафика</b>: " + str(users_org) + " человек. \n" +\
                            str(p_1) + "% от 1 день назад \n" + \
                            str(p_8) + "% от 7 дней назад \n"


            h2_2 = "\n\n<b>МЕТРИКИ</b> \n\n"


            #-----------------------------------------------CTR
            p_1 = round(metrics_yest['CTR'].values[0] * 100 / metrics_yest_1['CTR'].values[0] - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(metrics_yest['CTR'].values[0] * 100 / metrics_yest_8['CTR'].values[0] - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_ctr = "<b>CTR: </b>" + \
                            str(round(metrics_yest['CTR'].values[0], 3)) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от 7 дней назад \n\n"


            #-----------------------------------------------Views
            p_1 = round(metrics_yest['Views'].values[0] * 100 / metrics_yest_1['Views'].values[0] - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(metrics_yest['Views'].values[0] * 100 / metrics_yest_8['Views'].values[0] - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_views = "<b>Количество просмотров: </b>" + \
                            str(metrics_yest['Views'].values[0]) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от от 7 дней назад\n\n"

            #----------------------------------------------Likes
            p_1 = round(metrics_yest['Likes'].values[0] * 100 / metrics_yest_1['Likes'].values[0] - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(metrics_yest['Likes'].values[0] * 100 / metrics_yest_8['Likes'].values[0] - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_likes = "<b>Количество лайков: </b>" + \
                            str(metrics_yest['Likes'].values[0]) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от от 7 дней назад\n\n"

            #--------------------------------------------Unique posts
            p_1 = round(metrics_yest['uniq_posts'].values[0] * 100 / metrics_yest_1['uniq_posts'].values[0] - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(metrics_yest['uniq_posts'].values[0] * 100 / metrics_yest_8['uniq_posts'].values[0] - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_u_posts = "<b>Количество уникальных просмотренные постов: </b>" + \
                            str(metrics_yest['uniq_posts'].values[0]) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от от 7 дней назад\n\n"

            #--------------------------------------------Messages
            p_1 = round(metrics_yest['Messages'].values[0] * 100 / metrics_yest_1['Messages'].values[0] - 100, 2)
            if p_1 > 0:
                p_1 = str("+") + str(p_1)

            p_8 = round(metrics_yest['Messages'].values[0] * 100 / metrics_yest_8['Messages'].values[0] - 100, 2)
            if p_8 > 0:
                p_8 = str("+") + str(p_8)

            m_mess = "<b>Количество сообщений: </b>" + \
                            str(metrics_yest['Messages'].values[0]) + "\n" + \
                            str(p_1) + "% от 1 день назад" + "\n" + \
                            str(p_8) + "% от от 7 дней назад\n\n"

            #---------------------------------------------Plots
            #DAU,  New users
            bar = pd.DataFrame({'labels': ['только лента новостей', 'только мессенджер', 'и лента и мессенджер'],
                        'data' : [only_l, only_m, ml]})

            sns.set_style("whitegrid")
            figure, axes = plt.subplots(2, 2, figsize=(20, 12))

            plt.subplots_adjust(hspace=0.6)

            sns.lineplot(ax = axes[0, 0], x='Date', y='DAU', data=dau_df, marker= '.', markersize=10)
            axes[0, 0].set_title("DAU")
            axes[0, 0].set(xlabel='Дата')

            sns.barplot(ax = axes[0 , 1], data = bar, x='labels', y='data')
            axes[0, 1].tick_params(axis='x', rotation=45)
            axes[0, 1].set_title("Активность пользователей по разделам приложения")
            axes[0, 1].set(xlabel='', ylabel='Количество')

            sns.lineplot(ax = axes[1, 0], x='start_date', y='count', data=new_users[(new_users['source']=='ads')],  marker= '.', markersize=10)
            axes[1, 0].set_title("Новые пользователи с рекламного трафика")
            axes[1, 0].set(xlabel='Дата регистрации', ylabel='Количество')

            sns.lineplot(ax = axes[1, 1], x='start_date', y='count', data=new_users[(new_users['source']=='organic')],  marker= '.', markersize=10)
            axes[1, 1].set_title("Новые пользователи с органического трафика")
            axes[1, 1].set(xlabel='Дата регистрации', ylabel='Количество')

            plot_1 = io.BytesIO()
            plt.savefig(plot_1)
            plot_1.seek(0)
            plt.close()

            #CTR, likes,views,mess
            figure, axes = plt.subplots(2, 2, figsize=(20, 12))

            plt.subplots_adjust(hspace=0.6)

            sns.lineplot(ax = axes[0, 0], x='Date', y='CTR', data=metrics, marker= '.', markersize=10)
            axes[0, 0].set_title("CTR")
            axes[0, 0].set(xlabel='Дата')

            sns.lineplot(ax = axes[0, 1], data=metrics[['Date', 'Likes', 'Views']],  marker= '.', markersize=10)
            axes[0, 1].set_title("Лайки и просмотры")
            axes[0, 1].set(xlabel='Дата', ylabel='Количество')

            sns.lineplot(ax = axes[1, 0], x='Date', y='uniq_posts', data=metrics,  marker= '.', markersize=10)
            axes[1, 0].set_title("Уникальные просмотренные посты")
            axes[1, 0].set(xlabel='Дата', ylabel='Количество')

            sns.lineplot(ax = axes[1, 1], x='Date', y='Messages', data=metrics,  marker= '.', markersize=10)
            axes[1, 1].set_title("Сообщения")
            axes[1, 1].set(xlabel='Дата', ylabel='Количество')

            plot_2 = io.BytesIO()
            plt.savefig(plot_2)
            plot_2.seek(0)
            plt.close()



            message = header + h2_1 + m_dau + m_only + m_new_users + h2_2 + m_ctr + m_views + m_likes + m_u_posts + m_mess

            media_group = [InputMediaPhoto(media=plot_1), InputMediaPhoto(media=plot_2)]
            bot.sendMessage(chat_id=chat_id, text=message, parse_mode='html')
            bot.sendMediaGroup(chat_id=chat_id, media=media_group)
    



    metrics = get_metrics()
    dau = get_dau()
    new_users = get_new_users()
    m_l_users = get_users_m_l()
    create_message(metrics, new_users, dau, m_l_users, chat_channel)

anoshkina_bot_dag_1 = anoshkina_bot_dag_1()  
    

