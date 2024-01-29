import pandas as pd
from IPython.display import Image
from google.oauth2 import service_account
import pandas_gbq

#EXTRAÇÃO
empresas = pd.read_csv('tabela_empresas.csv')
datas = pd.read_csv('tabela_datas.csv')
contratos = pd.read_csv('tabela_contratos.csv')

#TRANSFORMAÇÃO
contratos_mod = contratos.merge(empresas, left_on='fk_empresa_contratada', right_on='id_empresa', how='left')
contratos_mod.drop(columns=['id_empresa', 'fk_empresa_contratada'], inplace=True)
contratos_final = contratos_mod.merge(datas, left_on='inicio_vigencia', right_on='id_data', how='left')
contratos_final.drop(columns=['inicio_vigencia', 'id_data'], inplace=True)
contratos_final.rename(columns={'data': 'data_inicio_vigencia'}, inplace=True)
df_final = contratos_final.merge(datas, left_on='termino_vigencia', right_on='id_data', how='left')
df_final.drop(columns=['termino_vigencia', 'id_data'], inplace=True)
df_final.rename(columns={'data': 'data_final_vigencia'}, inplace=True)
df_final.data_final_vigencia = df_final.data_final_vigencia.str.replace('31/09/2017', '30/09/2017')
df_final.data_inicio_vigencia = pd.to_datetime(df_final.data_inicio_vigencia, format='%d/%m/%Y').dt.date
df_final.data_final_vigencia = pd.to_datetime(df_final.data_final_vigencia, format='%d/%m/%Y').dt.date
df_final['tempo_contrato'] = (df_final.data_final_vigencia-df_final.data_inicio_vigencia).dt.days
df_final = df_final[df_final.tempo_contrato > 0]
df_final.reset_index(drop=True, inplace=True)

#CARREGAMENTO
credenciais = service_account.Credentials.from_service_account_file(
    filename='gbq.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])
df_final.to_gbq(credentials=credenciais, destination_table='curso_etl.etl_csv', if_exists='append',
                table_schema=[{'name': 'data_inicio_vigencia', 'type': 'DATE'},
                              {'name': 'data_final_vigencia', 'type': 'DATE'}])