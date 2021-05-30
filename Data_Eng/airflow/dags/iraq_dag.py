def read_data():
  import pandas as pd 
  Day='01-01-2021'
  URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
  DF_day=pd.read_csv(URL_Day)
  DF_day['Day']=Day
  cond=(DF_day.Country_Region=='Iraq')
  Selec_columns=['Day','Country_Region', 'Last_Update',
        'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
        'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
  DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
  return DF_i

def list_days():
  List_of_days=[]
  for year in range(2020,2022):
    for month in range(1,13):
      for day in range(1,32):
        month=int(month)
        if day <=9:
          day=f'0{day}'

        if month <= 9 :
          month=f'0{month}'
        List_of_days.append(f'{month}-{day}-{year}')
  return List_of_days

def Get_DF_i(Day):    
    DF_i=None
    try: 
        URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
        DF_day=pd.read_csv(URL_Day)
        DF_day['Day']=Day
        cond=(DF_day.Country_Region=='Iraq')
        Selec_columns=['Day','Country_Region', 'Last_Update',
              'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
              'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
        DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
    except:
      #print(f'{Day} is not available!')
      pass
    return DF_i

def append_days():
  import pandas as pd
  DF_all=[]
  for Day in list_days():
      #print(Day)
      DF_all.append(Get_DF_i(Day))

  DF_Iraq=pd.concat(DF_all).reset_index(drop=True)
  # Create DateTime for Last_Update
  DF_Iraq['Last_Updat']=pd.to_datetime(DF_Iraq.Last_Update, infer_datetime_format=True)  
  DF_Iraq['Day']=pd.to_datetime(DF_Iraq.Day, infer_datetime_format=True)  

  DF_Iraq['Case_Fatality_Ratio']=DF_Iraq['Case_Fatality_Ratio'].astype(float)

  return DF_Iraq

def scale_data():
  DF_Iraq_u=append_days()
  DF_Iraq_u.index=DF_Iraq_u.Day
  Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
  DF_Iraq_u_2=DF_Iraq_u[Selec_Columns]

  DF_Iraq_u_2

  from sklearn.preprocessing import MinMaxScaler

  min_max_scaler = MinMaxScaler()


  DF_Iraq_u_3 = pd.DataFrame(min_max_scaler.fit_transform(DF_Iraq_u_2[Selec_Columns]),columns=Selec_Columns)
  DF_Iraq_u_3.index=DF_Iraq_u_2.index
  DF_Iraq_u_3['Day']=DF_Iraq_u.Day
  return DF_Iraq_u_3

def plot_scoring_report():
  import matplotlib.pyplot as plt 
  import matplotlib
  font = {'weight' : 'bold',
        'size'   : 18}
  Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
  matplotlib.rc('font', **font)
  scale_data()[Selec_Columns].plot(figsize=(20,10))
  plt.savefig('Iraq_scoring_report.png')

def save_csv():
  scale_data().to_csv('Iraq_scoring_report.csv')
  DF_Iraq_u=append_days().copy()
  DF_Iraq_u.index=DF_Iraq_u.Day
  Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
  DF_Iraq_u_2=DF_Iraq_u[Selec_Columns]

  DF_Iraq_u_2
  DF_Iraq_u_2.to_csv('Iraq_scoring_report_NotScaled.csv')

def postgreSQL_table():
  from sqlalchemy import create_engine
  import psycopg2

  host="postgres" 
  database="testDB"
  user="me"
  password="1234"
  port='5432'
  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
  print(engine.table_names())
  Day='25_5_2021'
  DF_Iraq_u_3.to_sql(f'iraq_scoring_report_{Day}', engine,if_exists='replace',index=False)
  DF_Iraq_u_2.to_sql(f'iraq_scoring_notscaled_report_{Day}', engine,if_exists='replace',index=False) 

import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Asma'
    #,'start_date': dt.datetime(2021, 5, 10)
    #'retries': 1 
    #'retry_delay': dt.timedelta(minutes=10)
}
 
with DAG('iraq_scoring_report1',
         start_date=dt(2021, 5, 1),
         default_args=default_args,
         schedule_interval="@daily",  
        catchup=False     
         ) as dag:
 
    read_data = PythonOperator(task_id='reading',
                             python_callable=read_data)
    
    list_days = PythonOperator(task_id='listing',
                             python_callable=list_days)
    
    append_days = PythonOperator(task_id='append_days',
                             python_callable=append_days)
    
    scale_data = PythonOperator(task_id='scale_data',
                              python_callable=scale_data)
    
    postgreSQL_table = PythonOperator(task_id='postgreSQL_table',
                              python_callable=postgreSQL_table)
 
 

read_data >> list_days >> append_days >> scale_data >> postgreSQL_table 