import pandas as pd
import sqlalchemy
import psycopg2

server = '127.0.0.1'
db = 'retail_db'
usr = 'postgres'
passw = 'Mancity01.'
port = '5432'
ft = 'employee'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://"+usr+":"+passw+"@"+server+":"+port+"/"+db+"")

df = pd.read_csv(r'C:\Users\johan\AdventureWorks-oltp-install-script\employee.csv')

df.to_sql(ft,engine, index = False, if_exists = 'replace')