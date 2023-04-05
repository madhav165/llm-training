import pandas as pd

dfc = pd.read_csv('India_capacity_conventional_detail.csv')
dfr = pd.read_csv('India_capacity_renewable_detail.csv')

dfc['YYYY-MM-DD'] = dfc['YYYYMM'].apply(pd.to_datetime, format='%Y%m').apply(lambda x: x.date())
dfr['YYYY-MM-DD'] = dfr['YYYYMM'].apply(pd.to_datetime, format='%Y%m').apply(lambda x: x.date())

dfc = dfc.drop(columns='YYYYMM')
dfr = dfr.drop(columns='YYYYMM')

dfc = dfc.fillna(0)
dfr = dfr.fillna(0)

dfc = dfc.set_index('YYYY-MM-DD').stack().reset_index()
dfc = dfc.rename(columns={'level_1': 'State-GenType', 0: 'Capacity (MW)'})
dfc = dfc.assign(Type='Non-Renewable')
dfc[['State', 'GenType']] = dfc['State-GenType'].apply(lambda x: pd.Series(x.split('-')))
dfc = dfc.drop(columns='State-GenType')

dfr = dfr.set_index('YYYY-MM-DD').stack().reset_index()
dfr = dfr.rename(columns={'level_1': 'State-GenType', 0: 'Capacity (MW)'})
dfr = dfr.assign(Type='Renewable')
dfr[['State', 'GenType']] = dfr['State-GenType'].apply(lambda x: pd.Series(x.split('-')))
dfr = dfr.drop(columns='State-GenType')

dfc.to_csv('India_capacity_conventional_detail_v2.csv')
dfr.to_csv('India_capacity_renewable_detail_v2.csv')

df = pd.read_csv('India_capacity_detail_v2.csv')
df = df.groupby(['YYYY-MM-DD', 'State', 'Type', 'GenType'])['Capacity (MW)'].sum().reset_index()
df = df.rename(columns={'GenType': 'Source'})
df.to_csv('India_capacity_details_v3.csv', index=False)