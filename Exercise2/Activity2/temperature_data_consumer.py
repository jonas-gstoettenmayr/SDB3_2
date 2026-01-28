import time
from datetime import datetime, timedelta
import psycopg
from psycopg import sql

# -------------------------
# Periodically compute average over last 10 minutes
# -------------------------
DB_NAME = "office_db"
DB_USER = "postgres"
DB_PASSWORD = "postgrespw"
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_TABLE = "temperature_readings"
try:
    while True:
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        ## Fetch the data from the choosen source (to be implemented)

        result = []
        with psycopg.connect(
            dbname=DB_NAME,  # default DB
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        ) as conn:
            cursor = conn.cursor()
            cursor.execute(sql.SQL(f"SELECT * FROM {DB_TABLE} ORDER BY id DESC LIMIT 10") )
            result = cursor.fetchall()
            cursor.close()
        if result is None:
            print("No Data")

        temps = [x[2] for x in result]
        avg_temp = sum(temps)/len(temps) ## replace with actual values
        if avg_temp is not None:
            print(f"{datetime.now()} - Average temperature last 10 minutes: {avg_temp:.2f} °C")
        else:
            print(f"{datetime.now()} - No data in last 10 minutes.")
        time.sleep(600)  # every 10 minutes
except KeyboardInterrupt:
    print("Stopped consuming data.")
finally:
    print("Exiting.")
