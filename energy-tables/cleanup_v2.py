import pandas as pd

df = pd.read_csv('India_capacity_details_v3.csv')
df = df.groupby(['YYYY-MM', 'State', 'Type', 'Source'])['Capacity_MW'].sum().reset_index()

df2 =  df.pivot(index='YYYY-MM', columns=['State', 'Type', 'Source'], values='Capacity_MW')
df2 = df2.fillna(method='ffill')
df2 = df2.stack().stack().stack().reset_index()
df2.to_csv('India_capacity_details_v5.csv', index=False)
