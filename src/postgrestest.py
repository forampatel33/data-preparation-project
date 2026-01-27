import pyarrow
import pandas
import psycopg

conn = psycopg.connect(
    dbname="nyc_taxi",
    user="postgres",
    host="/var/run/postgresql"
)

cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())

cur.close()
conn.close()
