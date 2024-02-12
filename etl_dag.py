from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': '',
    'password': '',
    'user': '',
    'database': ''
}


connection_test = {'host': '',
                      'database':'test',
                      'user':'',
                      'password':''
                     }




default_args={
            'owner': 'L.Anoshkina',
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=3),
            'start_date': datetime(2024, 1, 13),
        }

    
schedule_interval = timedelta(days=1)
        
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def anoshkina_dag():
    
    @task()
    def get_likes_views_per_user():
        q = """
            SELECT user_id, 
                    sum(action = 'view') AS views,
                    sum(action = 'like') AS likes         
            FROM simulator_20231220.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id
            """
        res = ph.read_clickhouse(q, connection=connection)
        return res
    
    @task()
    def get_messages_per_user():
        q = """
        SELECT user_id,  messages_received, messages_sent, users_received, users_sent FROM

            (SELECT user_id, count(user_id) as messages_sent, count(DISTINCT receiver_id) as users_sent
            FROM simulator_20231220.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY user_id) t1

        join

            (SELECT receiver_id, count(receiver_id) as messages_received, count(DISTINCT user_id) as users_received
            FROM simulator_20231220.message_actions
            WHERE toDate(time) = yesterday()
            GROUP BY receiver_id) t2

        ON (t1.user_id = t2.receiver_id) 
        ORDER BY user_id
        """
        res = ph.read_clickhouse(q, connection=connection)
        return res
    
    @task()
    def merge_table(t1, t2):
        q = """ 
        SELECT DISTINCT user_id, age, gender, os
        FROM simulator_20231220.feed_actions
        """
        users_info =  ph.read_clickhouse(q, connection=connection)
        
        merged = t1.merge(t2, on='user_id', how='outer')
        merged = merged.merge(users_info, on='user_id', how='inner')
        merged = merged.fillna(0)
        print(merged.to_csv(index=False, sep='\t'))
        return merged
    
    @task()
    def section_os(df):
        section_table_os = df.drop(['user_id', 'age', 'gender'], axis=1)
        section_table_os = section_table_os.groupby(by=['os']).sum().reset_index().rename(columns={'os': 'dimension_value'})
        section_table_os.insert(0, 'event_date', datetime.today().strftime("%Y-%m-%d"), False)
        section_table_os.insert(1, 'dimension', 'os', False)
        return section_table_os
    
    @task()
    def section_gender(df):
        section_table_gender = df.drop(['user_id', 'age', 'os'], axis=1)
        section_table_gender = section_table_gender.groupby(by=['gender']).sum().reset_index().rename(columns={'gender': 'dimension_value'})
        section_table_gender.insert(0, 'event_date', datetime.today().strftime("%Y-%m-%d"), False)
        section_table_gender.insert(1, 'dimension', 'gender', False)
        return section_table_gender
    
    @task() 
    def section_age(df):
        section_table_age = df.drop(['user_id', 'gender', 'os'], axis=1)
        section_table_age = section_table_age.groupby(by=['age']).sum().reset_index().rename(columns={'age': 'dimension_value'})
        section_table_age.insert(0, 'event_date', datetime.today().strftime("%Y-%m-%d"), False)
        section_table_age.insert(1, 'dimension', 'age', False)
        return section_table_age
    
    @task()
    def upload(df1, df2, df3):

        query_test = """CREATE TABLE IF NOT EXISTS test.anoshkina
                            (
                            event_date date,
                            dimension String ,
                            dimension_value String ,
                            views UInt64,
                            likes UInt64,
                            messages_received UInt64,
                            messages_sent UInt64,
                            users_received UInt64,
                            users_sent UInt64
                            )
                            ENGINE = MergeTree()
                            ORDER BY event_date
        """

        ph.execute(query_test, connection=connection_test)
        
        final_table = pd.concat([df1, df2, df3])
        columns = ['views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']
        final_table[columns] = final_table[columns].astype(int)
        
        ph.to_clickhouse(df=final_table, table="anoshkina", index=False, connection=connection_test)
        
    
    likes_views_per_user = get_likes_views_per_user()
    messages_per_user = get_messages_per_user()
    
    merge_table = merge_table(likes_views_per_user, messages_per_user)
    
    section_os = section_os(merge_table)
    section_gender = section_gender(merge_table)
    section_age = section_age(merge_table)
      
    upload(section_os, section_gender, section_age)
    
anoshkina_dag = anoshkina_dag()
    
 
    


  