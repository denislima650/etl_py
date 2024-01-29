import pandas as pd
from functools import reduce
from google.oauth2 import service_account

unid_orc = pd.read_json('tb_unidades_orcamentarias.json')
anos = pd.read_json('tb_anos.json')
acoes = pd.read_json('tb_acoes.json')
programas = pd.read_json('tb_programas.json')
orcamentos = pd.read_json('tb_orcamentos.json')

orcamentos = orcamentos.merge(acoes, left_on='fk_acao', right_on='id_acao', how='left')
orcamentos = orcamentos.merge(programas, left_on='fk_programa', right_on='id_programa', how='left')
orcamentos = orcamentos.merge(anos, left_on='fk_ano', right_on='id_ano', how='left')
orcamentos = orcamentos.merge(unid_orc, left_on='fk_unidade', right_on='id_unidade', how='left')

orcamentos.drop(columns=['id_unidade', 'fk_unidade', 'fk_ano', 'id_ano',
                         'id_programa', 'fk_programa', 'id_acao', 'fk_acao'], inplace=True)

#CARREGAMENTO
credenciais = service_account.Credentials.from_service_account_file(
    filename='gbq.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])

orcamentos.to_gbq(credentials=credenciais, destination_table='curso_etl.etl_json', if_exists='replace')