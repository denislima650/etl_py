import pandas as pd
from functools import reduce
from google.oauth2 import service_account

#Extração
atletas = pd.read_excel('Athletes.xlsx')
tecnicos = pd.read_excel('Coaches.xlsx')
ent_gen = pd.read_excel('EntriesGender.xlsx')
times = pd.read_excel('Teams.xlsx')
medalhas = pd.read_excel('Medals.xlsx')

#Transformação
atletas_disc = atletas.groupby(['NOC', 'Discipline'])['Name'].nunique() #atletas por modalid. e pais
grupo_atletas = pd.DataFrame(atletas_disc).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'num_atletas'})
grupo_tecnicos = pd.DataFrame(tecnicos.groupby(['NOC', 'Discipline'])['Name'].nunique()).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'num_tecnicos'})
times['categoria'] = times.Event.apply(lambda x: 'Masculino' if 'Men' in x
                                       else ('Feminino' if 'Women' in x
                                             else('Misto' if 'Mixed' in x
                                                  else 'Outros')))
grupo_masculino = pd.DataFrame(times[times.categoria=='Masculino'].groupby(['NOC', 'Discipline'])['Name'].nunique()).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'times_masculinos'})
grupo_feminino = pd.DataFrame(times[times.categoria=='Feminino'].groupby(['NOC', 'Discipline'])['Name'].nunique()).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'times_femininos'})
grupo_misto = pd.DataFrame(times[times.categoria=='Misto'].groupby(['NOC', 'Discipline'])['Name'].nunique()).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'times_mistos'})
grupo_outros = pd.DataFrame(times[times.categoria=='Outros'].groupby(['NOC', 'Discipline'])['Name'].nunique()).reset_index().rename(columns={'NOC': 'PAIS',
                                                                         'Discipline': 'Modalidade',
                                                                         'Name': 'times_outros'})
#juntando tabelas
df = [grupo_atletas, grupo_tecnicos, grupo_masculino, grupo_feminino, grupo_misto, grupo_outros]
df_final = reduce(lambda left, right: pd.merge(left, right, on=['PAIS', 'Modalidade'], how='outer'), df).fillna(0)
print(df_final[df_final.PAIS == 'Brazil'])

#CARREGAMENTO
credenciais = service_account.Credentials.from_service_account_file(
    filename='gbq.json', scopes=['https://www.googleapis.com/auth/cloud-platform'])

df_final.to_gbq(credentials=credenciais, destination_table='curso_etl.etl_excel', if_exists='replace')