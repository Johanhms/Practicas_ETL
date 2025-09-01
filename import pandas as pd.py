import pandas as pd
import sqlalchemy
import psycopg2

server = '127.0.0.1'
db = 'test_db'
usr = 'postgres'

port= '5432'
ft = 'addtype'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{usr}:@{server}:{port}/{db}")


df = pd.read_csv(r'C:\Users\johan\AdventureWorks-oltp-install-script\AddressType.csv')


df.to_sql(ft,engine,if_exists='replace',index=False)

