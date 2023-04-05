import pandas as pd
import re
from typing import Set
from dotenv import load_dotenv
from transformers import GPT2TokenizerFast
import numpy as np
import openai
import os
import json
from nltk.tokenize import sent_tokenize
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    wait_random
)  # for exponential backoff

load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')

tokenizer = GPT2TokenizerFast.from_pretrained("gpt2")
EMBEDDING_MODEL = "text-embedding-ada-002"

def count_tokens(text: str) -> int:
    """count the number of tokens in a string"""
    return len(tokenizer.encode(text))

@retry(wait=wait_random_exponential(min=1, max=10), stop=stop_after_attempt(10))
# @retry(wait=wait_random(min=60, max=120), stop=stop_after_attempt(10))
def get_embedding(text: str, model: str=EMBEDDING_MODEL) -> list[float]:
    result = openai.Embedding.create(
      model=model,
      input=text
    )
    return result["data"][0]["embedding"]

df = pd.read_csv('India_capacity_details_v3.csv')

df['YYYY-MM-DD State Type Source Capacity (MW)'] = df.apply(lambda x: f"{x['YYYY-MM-DD']} {x['State']} {x['Type']} {x['Source']} {x['Capacity (MW)']}", axis=1)
df['tokens'] = df['YYYY-MM-DD State Type Source Capacity (MW)'].apply(count_tokens)

df2 = df.loc[df['State'].isin(['Andhra Pradesh', 'Telangana'])].reset_index(drop=True)
df2 = df.iloc[:50].reset_index(drop=True)

g = df2.groupby(['YYYY-MM-DD', 'State', 'Type'])
num_pages = df2.shape[0]
arr = []
j_arr = []
for index, k in enumerate(g.groups.keys()):
    klist = list(k)
    print(klist)
    dfl = df.iloc[g.groups[k]]
    content = 'YYYY-MM-DD State Type Source Capacity (MW)\n'
    content += '\n'.join(dfl['YYYY-MM-DD State Type Source Capacity (MW)'].tolist())
    embedding = get_embedding(content)
    klist.extend(embedding)
    arr.append(klist)
    d = {}
    d['id'] = f'item_{index}'
    d['score'] = 0
    d['values'] = embedding
    d2 = {}
    d2['pdf_numpages'] = num_pages
    d2['source'] = k
    d2['text'] = content
    d['metadata'] = d2
    j_arr.append(d)

df_emb = pd.DataFrame(arr)
cols = ['YYYY-MM-DD', 'State', 'Type']
cols.extend(range(1536))
df_emb.columns = cols
df_emb.to_csv('india_power_sample_embeddings.csv', index=False)

j = {}
j['vectors'] = j_arr
j['namespace'] = 'india_power_sample'

with open('india_power_sample.json', 'w') as f:
    json.dump(j, f)
