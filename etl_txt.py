import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import matplotlib as mp
import seaborn as sb
from google.oauth2 import service_account

df = pd.read_csv('NYNEWYOR.txt', delim_whitespace=True, header=None)
df.rename(columns={0: 'mes', 1: 'dia', 2: 'ano', 3: 'temperatura'}, inplace=True)

print(df.describe())

df.temperatura = df.temperatura.replace(-99, np.NAN)

df['elemento_anterior'] = df.temperatura.shift(1)
df['elemento_posterior'] = df.temperatura.shift(-1)

df['back_fill'] = df.temperatura.bfill(axis=0)
df['forward_fill'] = df.temperatura.ffill(axis=0)
df['substituicao'] = df.temperatura.shift(1).ffill(axis=0)
df.temperatura = np.where(df.temperatura.notnull() == False, df.substituicao, df.temperatura)

plt.Figure(figsize=(30, 10))
sb.lineplot(df.temperatura)
plt.show()

#CARREGAMENTO
credenciais = service_account.Credentials.from_service_account_file(
    filename='gbq.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])

df.to_gbq(credentials=credenciais, destination_table='curso_etl.etl_txt')

