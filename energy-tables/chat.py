import os
import pandas as pd
from dotenv import load_dotenv
import openai
import psycopg2

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')
psql_host = os.getenv('PSQL_HOST')
psql_port = os.getenv('PSQL_PORT')
psql_db = os.getenv('PSQL_DB')
psql_user = os.getenv('PSQL_USER')
psql_password = os.getenv('PSQL_PASSWORD')

COMPLETIONS_MODEL = "gpt-3.5-turbo"
# COMPLETIONS_MODEL = "gpt-4"
context = None
message_list = []
err = ''

def get_result(sql_statement):
    global err
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
        err = e
    finally:
        if conn is not None:
            conn.close()
        return df_res

def construct_prompt(question: str) -> str:
    if len(message_list) == 0 and context is None:
        header = """Answer the question as truthfully as possible using the provided context, 
        and if the answer is not specified within the text below, your response should only contain 
        Postgres SQL queries for querying a table named details with the following columns.
        
            yyyy_mm - For storing year and month. get latest as max(yyyy_mm)
            state - State in India
            type - Renewable or Non-Renewable
            source - Energy source (Bio Bagasse Cogen, Bio Other Cogen, Bio Waste, Bio Waster Offgrid, Hydro, Natural Gas, Nuclear, Small Hydro, Solar Ground, Solar Ground Based, Solar Hybrid, Solar Offgrid, Solar Rooftop, Thermal, Wind)
            capacity - Power generation capacity in MW

        \nThink of all the columns needed for a human to answer the question and bring them in.
        \nAlways rename the output columns to be descriptive of the data.
        "\n\nContext (result returned by SQL query):\nAssume the data is for the requested time period, region, source or type\n"""
        if context is not None:
            header += context
    else:
        if context is not None:
            header = """Answer the question as truthfully as possible using the provided context, 
        and if the answer is not specified within the text below, say "I don't know".\n\n"Context:\n""" + context
        else:
            header = ""

    return header + "\n\n Q: " + question + "\n A:"

def answer_query(
        query: str,
        show_prompt: bool = False,
        clear_message_list = False,
        clear_context = False
    ) -> str:
    global message_list
    global context
    if clear_message_list:
        message_list = []
    if clear_context:
        context = None
    prompt = construct_prompt(
        query
    )
    
    message_list.append({'role': 'user', 'content': prompt})
    
    if show_prompt:
        print(prompt)

    response = openai.ChatCompletion.create(
    model=COMPLETIONS_MODEL,
    messages=message_list
    )

    return response["choices"][0]["message"]["content"].strip(" \n")

def answer_qtn_from_table(qtn):
    c = 0
    global context
    global err
    err = ''
    context = None
    while context is None:
        query = answer_query(qtn, True, True)
        print(query)
        df_res = get_result(query)
        if df_res.shape[0] > 0:
            context_arr = ['|'.join(df_res.columns.tolist())]
            for i, r in df_res.iterrows():
                context_arr.append('|'.join(r.values.astype(str)))
            context = '\n'.join(context_arr)
            print(df_res)
            break
        if df_res.shape[0] == 0:
            c += 1
            query = answer_query(f'Got an error running the code:  {err}.\nReturn a corrected query', True)
        if c==5:
            break
    resp = answer_query(qtn+' Try to summarize.', True, True, False)
    return resp

qtn = "How is the breakup of energy sources of Karnataka compared to Andhra Pradesh and Telangana in the latest month?"
resp = answer_qtn_from_table(qtn)
print(resp)

qtn = "How is the breakup of energy sources of Karnataka compared to Andhra Pradesh and Telangana?"
resp = answer_query(qtn, True, True, False)
print(resp)

qtn = "In which sources is Telangana ahead of Andhra Pradesh and Karnataka?"
resp = answer_qtn_from_table(qtn)
print(resp)


qtn = "List the top 5 states with nuclear source capacity in the latest month"
resp = answer_qtn_from_table(qtn)
print(resp)

qtn = "Which energy sources had the highest CAGR in Maharashtra? Identify the top ones for renewables and non-renewables separately"
resp = answer_qtn_from_table(qtn)
print(resp)

