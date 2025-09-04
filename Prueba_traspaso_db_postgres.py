
import pandas as pd
import sqlalchemy
import psycopg2

server = '127.0.0.1'
db = 'retail_db'
usr = 'postgres'
passw = 'password'  
port = '5432'
tabla_inicial = 'products'

engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{usr}:{passw}@{server}:{port}/{db}")




server2 = '127.0.0.1'
db2 = 'test_db'
usr2 = 'postgres'
passw2 = 'password'  
port2 = '5432'
tabla_final = 'prodct'

engine2 = sqlalchemy.create_engine(f"postgresql+psycopg2://{usr2}:{passw2}@{server2}:{port2}/{db2}")

df = pd.read_sql(tabla_inicial, engine.connect())

df.to_sql(tabla_final,engine2)
