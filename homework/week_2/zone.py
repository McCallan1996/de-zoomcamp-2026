import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv('taxi_zone_lookup.csv')
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
df.to_sql(name='taxi_zone', con=engine, if_exists='replace')

print("Table created and Data injected successfully.")
