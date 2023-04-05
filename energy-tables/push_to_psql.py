import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2

load_dotenv()
psql_host = os.getenv('PSQL_HOST')
psql_port = os.getenv('PSQL_PORT')
psql_db = os.getenv('PSQL_DB')
psql_user = os.getenv('PSQL_USER')
psql_password = os.getenv('PSQL_PASSWORD')

def get_result(sql_statement):
    df_res = pd.DataFrame()
    try:
        conn = psycopg2.connect(
            host=psql_host,
            port=psql_port,
            dbname=psql_db,
            user=psql_user,
            password=psql_password
        )

        cur = conn.cursor()
        cur.execute(sql_statement)
        res = cur.fetchall()
        df_res = pd.DataFrame(res)
        column_names = [i[0] for i in cur.description]
        df_res.columns = column_names

        cur.close()
    except Exception as e:
        print(e)
    finally:
        if conn is not None:
            conn.close()
        return df_res

sql_statement = '''
SELECT 
    state, 
    100 * SUM(CASE WHEN type = 'Renewable' THEN capacity ELSE 0 END) / SUM(capacity) AS renewable_share
FROM 
    details
WHERE 
    yyyy_mm = (SELECT MAX(yyyy_mm) FROM details)
GROUP BY 
    state
ORDER BY 
    renewable_share DESC
LIMIT 
    10;
'''

sql_statement = '''
WITH yearly_totals AS (
    SELECT 
        state,
        source,
        DATE_TRUNC('year', TO_DATE("yyyy_mm", 'YYYY-MM')) AS year,
        SUM(capacity) AS yearly_capacity
    FROM 
        details
    GROUP BY 
        state, 
        source, 
        DATE_TRUNC('year', TO_DATE("yyyy_mm", 'YYYY-MM\'))
)
SELECT 
    state, 
    source, 
    POWER((MAX(yearly_capacity) / MIN(yearly_capacity)), 1.0 / COUNT(DISTINCT year)) - 1 AS cagr
FROM 
    yearly_totals
GROUP BY 
    state, 
    source;

'''

sql_statement = '''
WITH yearly_totals AS (
    SELECT 
        state,
        source,
        DATE_TRUNC('year', TO_DATE("yyyy_mm", 'YYYY-MM')) AS year,
        SUM(capacity) AS yearly_capacity
    FROM 
        details
    GROUP BY 
        state, 
        source, 
        DATE_TRUNC('year', TO_DATE("yyyy_mm", 'YYYY-MM'))
), latest_month AS (
    SELECT 
        state,
        source, 
        SUM(capacity) AS latest_month_capacity
    FROM 
        details
    WHERE 
        "yyyy_mm" = (SELECT MAX("yyyy_mm") FROM details)
    GROUP BY 
        state,
        source
    HAVING 
        SUM(capacity) >= 1000
)
SELECT 
    yearly_totals.state, 
    yearly_totals.source, 
    POWER((MAX(yearly_capacity) / MIN(yearly_capacity)), 1.0 / COUNT(DISTINCT year)) - 1 AS cagr
FROM 
    yearly_totals
    INNER JOIN latest_month ON yearly_totals.state = latest_month.state AND yearly_totals.source = latest_month.source
WHERE 
    yearly_totals.yearly_capacity > 0
GROUP BY 
    yearly_totals.state, 
    yearly_totals.source;

'''

sql_statement = '''
WITH renewable_share AS (
    SELECT 
        state,
        DATE_TRUNC('month', TO_DATE("yyyy_mm", 'YYYY-MM')) AS month,
        SUM(CASE WHEN type = 'Renewable' THEN capacity ELSE 0 END) AS renewable_capacity,
        SUM(capacity) AS total_capacity
    FROM 
        details
    GROUP BY 
        state, 
        DATE_TRUNC('month', TO_DATE("yyyy_mm", 'YYYY-MM'))
),
state_averages AS (
    SELECT 
        state,
        AVG(CASE WHEN total_capacity = 0 THEN NULL ELSE renewable_capacity * 100.0 / total_capacity END) AS average_renewable_share,
        DATE_TRUNC('month', month) AS month
    FROM 
        renewable_share
    GROUP BY 
        state, 
        DATE_TRUNC('month', month)
),
state_pct_changes AS (
    SELECT 
        a.State, 
        a.month, 
        CASE 
            WHEN b.average_renewable_share = 0 THEN NULL 
            ELSE ((a.average_renewable_share / b.average_renewable_share) - 1) * 100 
        END AS pct_change
    FROM 
        state_averages a
        JOIN state_averages b ON a.state = b.state AND a.month = (b.month + INTERVAL '1 month')
)
SELECT 
    state, 
    AVG(pct_change) AS avg_pct_change
FROM 
    state_pct_changes
GROUP BY 
    state
ORDER BY 
    avg_pct_change DESC;

'''

res = get_result(sql_statement)
res
res.to_csv('res4.csv', index=False)




explain which states have a bigger push towards renewables?

state	avg_pct_change
Lakshadweep	
Goa	880.401334
Uttar Pradesh	180.3589425
Jharkhand	62.2669472
Telangana	30.76103701
Haryana	16.13107458
Chhatisgarh	6.883680655
Chandigarh	1.39E-15
Others	7.40E-16
Daman & Diu	0
Lakshwadeep	0
Dadar & Nagar Haveli	0
Punjab	-3.157226916
Assam	-3.299397626
Odisha	-8.052168102
West Bengal	-10.27397934
Bihar	-12.35902874
Ladakh	-12.69986967
Andaman & Nicobar	-13.4115075
Manipur	-13.54310565
Uttarakhand	-13.94858813
Kerala	-15.70120951
Rajasthan	-16.53063582
Madhya Pradesh	-18.68801137
Gujarat	-19.19869425
Delhi	-19.2120078
Tripura	-19.36973915
Andhra Pradesh	-19.40870717
Maharashtra	-19.78862208
Karnataka	-19.97420779
Meghalaya	-20.32255787
Jammu & Kashmir	-20.4156861
Tamil Nadu	-20.41572955
Mizoram	-20.636615
Himachal Pradesh	-20.85120278
Nagaland	-20.97580464
Sikkim	-21.46762705
Arunachal Pradesh	-22.4877957
Pondicherry	-27.96604954
