# ======================= #
# === RAIS - DATABASE === #
# ======================= #

# --- Script by: Paulo Icaro --- #


# ================== #
# === Libraries  === #
# ================== #
import psycopg2 as sql        # For manipulating postgres database
import pandas as pd           # For data manipulations
import os                     # For acessing current o.s path
from pathlib import Path      # For acessing folder files
from io import StringIO       # For increasing data insertion performance

# ======================= #
# === Pre Adjustments === #
# ======================= #
rais_folder = Path(os.getcwd() + '/Microdados_Rais/')
year = 2014
counter = 0

# ======================= #
# === Data Processing === #
# ======================= #
rais_ceara = pd.DataFrame()
rais_files = sorted(rais_folder.glob('*.txt'))          # Sorted function is for sorting the .txt files
# list(rais_files)                                      # In case you want to see the files

# --- Reading databases + Few adjustments --- #
for file in rais_files:
    counter += 1
    rais = pd.read_csv(filepath_or_buffer = file, sep = ';', encoding = 'latin1')#, dtype = {'Bairros SP': str, 'Bairros Fortaleza':str, 'Bairros SP':str, 'CNAE 95 Classe':str})
    rais = rais[rais.UF == 23]
    rais = rais.apply(lambda x: x.astype(str).str.strip() if x.dtype == 'object' else x, axis = 0)
    rais['comp'] = year + counter
    rais_ceara = pd.concat([rais, rais_ceara])


# --- Add 0's to subclasse with less than 6 characters --- #
'''The zfill function is essencial here'''
rais_ceara['CNAE 2.0 Subclasse'] = rais_ceara['CNAE 2.0 Subclasse'].astype(str).str.zfill(7)

''' Another less efficient way to tho the same'''
#for t in range(len(rais_ceara)):
#    while len(str(rais_ceara['CNAE 2.0 Subclasse'].iloc[t])) < 7:
#        rais_ceara['CNAE 2.0 Subclasse'].iloc[t] = '0' + str(rais_ceara['CNAE 2.0 Subclasse'].iloc[t])


# --- Full Cleasing --- #
del rais_folder, rais_files, rais, file, counter, year



# ==================== #
# === SQL Database === #
# ==================== #

# --- Arguments vs Key Arguments --- #
# See: https://www.psycopg.org/docs/
# See: https://naysan.ca/2019/11/02/from-pandas-dataframe-to-sql-table-using-psycopg2/
# - Arguments: *args allows for any number of optional positional arguments (parameters), which will be assigned to a tuple named args.
# - Key Arguments:  **kwargs allows for any number of optional keyword arguments (parameters), which will be in a dict named kwargs.
''' In this case I use **db_credential so the info is properly inserted in the key arguments '''

# ------------------------------------------ #
# --- Creating database dw_caged_publica --- #
# ------------------------------------------ #
'''Run this snippet just in case it is the first time the database is created'''

db_credentials = {'host':'localhost', 'user':'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'postgres'}

conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()
cursor_conn.execute(query = 'drop database if exists db_rais_publica;')
cursor_conn.execute(query = 'create database db_rais_publica;')
cursor_conn.close()
conn.close()



# --------------------------------------------------------------- #
# --- Acessing database db_caged_publica and inserting tables --- #
# --------------------------------------------------------------- #
db_credentials = {'host': 'localhost', 'user':'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'db_rais_publica'}    # Database credentials
conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()

# --- Dropping tables --- #
cursor_conn.execute(query = 'drop table if exists tbl_rais_estab;')   

# --- (Re)Creating tables --- #
cursor_conn.execute(query = '''create table tbl_rais_estab
                    (bairros_sp varchar(10),
                     bairros_fortaleza varchar(10),
                     bairros_rj varchar(10),
                     cnae_classe varchar(10),
                     cnae_classe_95 varchar(10),
                     distritos_sp varchar(10),
                     qtd_vinculos_clt int not null,
                     qtd_vinculos_ativos int not null,
                     qtd_vinculos_estatutarios int not null,
                     ind_atividade_ano smallint not null,
                     ind_cei_vinculado smallint not null,
                     ind_estab_participa_pat smallint not null,
                     ind_rais_negativa smallint,
                     ind_simples smallint,
                     municipio varchar(6) not null,
                     nat_jur varchar(4),
                     regioes_adm_df smallint,
                     cnae_subclasse varchar(7) not null,
                     tam_estab smallint,
                     tipo_estab smallint,
                     tipo_estab_1 varchar(10),
                     uf smallint not null,
                     ibge_subsetor smallint,
                     cep_estab varchar(8),
                     compet smallint not null
                     );''')
                    
# --- Loading data do Postgres --- #
buffer = StringIO()
rais_ceara.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_rais_estab', sep = ';',
                      columns = ('bairros_sp', 'bairros_fortaleza', 'bairros_rj', 'cnae_classe',
                                 'cnae_classe_95', 'distritos_sp', 'qtd_vinculos_clt', 'qtd_vinculos_ativos',
                                 'qtd_vinculos_estatutarios', 'ind_atividade_ano', 'ind_cei_vinculado',
                                 'ind_estab_participa_pat', 'ind_rais_negativa', 'ind_simples', 'municipio',
                                 'nat_jur', 'regioes_adm_df', 'cnae_subclasse', 'tam_estab', 'tipo_estab',
                                 'tipo_estab_1', 'uf', 'ibge_subsetor', 'cep_estab', 'compet'))

cursor_conn.close()
conn.close()

del buffer, db_credentials, conn, cursor_conn#, rais_ceara