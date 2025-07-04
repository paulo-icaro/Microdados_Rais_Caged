# ========================= #
# === CAGED - DATABASE  === #
# ========================= #

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
caged_folder = Path(os.getcwd() + '/Microdados_Caged/') # Folder where databases lie
print(caged_folder.exists())                            # Check whether the folder exists
caged_exc = caged_for = caged_mov = pd.DataFrame()      # Empty data frames
caged_file_kind = ['EXC', 'FOR', 'MOV']                 # Databases type
counter = 0                                             # Counter for knowing how many databases are already read


# ======================= #
# === Data Processing === #
# ======================= #
'''
This loop accesses, filters and aggregates each database type sequencialy
After this process, there'll be three dataframes: caged_exc, caged_for and caged_mov
Each database will contain an agreggation of the database types file
'''
for i in caged_file_kind:
   caged_files = sorted(caged_folder.glob('CAGED' + i + '*.txt'))   # Sorted function is for sorting the .txt files
   caged_agr = pd.DataFrame()
   
   for file in caged_files:
       counter += 1
       caged = pd.read_csv(filepath_or_buffer = file, sep = ';')    # Read file
       caged = caged[caged.uf == 23]                                # Filter Ceara state only

       caged_agr = pd.concat([caged, caged_agr])                    # Agreggation
   
    
   # --- Add 0's to subclasse with less than 6 characters --- #
   '''The zfill function is essencial here'''
   caged_agr['subclasse'] = caged_agr['subclasse'].astype(str).str.zfill(7)
   
    
   # --- Databases division + Few adjustments --- #
   if caged_agr is not None:
       if i == 'EXC': 
            caged_exc = caged_agr
            caged_exc = caged_exc.replace(to_replace = ',', value = '.', regex = True)      # Replacing commas by dots
            caged_exc['idade'] = caged_exc['idade'].fillna(0).astype(int)                   # Changing variable type
            caged_exc['salário'] = caged_exc['salário'].fillna(0).astype(float)             # Adjusting empty wage values
            caged_exc['valorsaláriofixo'] = caged_exc['valorsaláriofixo'].fillna(0).astype(float)   # Adjusting empty wage values
            
       elif i == 'FOR':
            caged_for = caged_agr
            caged_for = caged_for.replace(',', '.', regex = True)
            caged_for['idade'] = caged_for['idade'].fillna(0).astype(int)
            caged_for['salário'] = caged_for['salário'].fillna(0).astype(float)           
            caged_for['valorsaláriofixo'] = caged_for['valorsaláriofixo'].fillna(0).astype(float)
       else:
            caged_mov = caged_agr
            caged_mov = caged_mov.replace(',', '.', regex = True)
            caged_mov['idade'] = caged_mov['idade'].fillna(0).astype(int)
            caged_mov['salário'] = caged_mov['salário'].fillna(0).astype(float)           
            caged_mov['valorsaláriofixo'] = caged_mov['valorsaláriofixo'].fillna(0).astype(float)
       del caged, caged_agr                                          # Cleasing

# --- Full Cleasing --- #       
del caged_file_kind, caged_files, caged_folder, file, i, counter



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

db_credentials = {'host': 'localhost', 'user':'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'postgres'}    # Database credentials

conn = sql.connect(**db_credentials)                                # The connect class
conn.autocommit = True                                              # All sql instructions will be automatically made (commited) in the database.
cursor_conn = conn.cursor()                                         # The class cursor allows the user to execute sql commands and queries
cursor_conn.execute(query = 'drop database if exists db_caged_publica;')
cursor_conn.execute(query = 'create database db_caged_publica;')
cursor_conn.close()                                                 # Remember to always close the cursor
conn.close()                                                        # Dont forget to also close the database connection

''' Just representing how we change the database credential to reconnect the acess'''
# db_credentials.update({'database':'dw_caged_publica'})            # Switching database credentials on key arguments for acessing with the correct info
# conn = sql.connect(**db_credentials)


# --------------------------------------------------------------- #
# --- Acessing database db_caged_publica and inserting tables --- #
# --------------------------------------------------------------- #
db_credentials = {'host': 'localhost', 'user':'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'db_caged_publica'}    # Database credentials
conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()

# --- Droping tables --- #
cursor_conn.execute(query = 'drop table if exists tbl_caged_exc;')  # Vacuum use not necessary
cursor_conn.execute(query = 'drop table if exists tbl_caged_for;')
cursor_conn.execute(query = 'drop table if exists tbl_caged_mov;')

# --- (Re)Creating tables --- #
cursor_conn.execute('''create table tbl_caged_exc 
                    (competencia_mov varchar(6) not null,
                     regiao smallint not null,
                     uf smallint not null,
                     municipio varchar(6) not null,
                     cnae_secao varchar(1),
                     cnae_subclasse varchar(7) not null,
                     saldo_movimentacao smallint not null,
                     cbo2002_ocupacao varchar(6),
                     categoria varchar(3),
                     grau_instrucao smallint,
                     idade smallint,
                     horas_contratuais varchar(15),
                     raca_cor smallint,
                     sexo smallint,
                     tipo_empregador smallint,
                     tipo_estabelecimento smallint,
                     tipo_movimentacao smallint,
                     tipo_deficiencia smallint,
                     ind_trab_intermitente smallint,
                     ind_trab_parcial smallint,
                     salario numeric(10,2),
                     tam_estab_jan smallint,
                     ind_aprendiz smallint,
                     origem_informacao smallint,
                     comp_dec varchar(6),
                     comp_exc varchar(6),
                     ind_exc smallint,
                     ind_fora_prazo smallint,
                     unid_salario_cod smallint,
                     salario_fixo numeric(10,2)
                     );''')

cursor_conn.execute('''create table tbl_caged_for
                    (competencia_mov varchar(6) not null,
                     regiao smallint not null,
                     uf smallint not null,
                     municipio varchar(6) not null,
                     cnae_secao varchar(1),
                     cnae_subclasse varchar(7) not null,
                     saldo_movimentacao smallint not null,
                     cbo2002_ocupacao varchar(6),
                     categoria varchar(3),
                     grau_instrucao smallint,
                     idade smallint,
                     horas_contratuais varchar(15),
                     raca_cor smallint,
                     sexo smallint,
                     tipo_empregador smallint,
                     tipo_estabelecimento smallint,
                     tipo_movimentacao smallint,
                     tipo_deficiencia smallint,
                     ind_trab_intermitente smallint,
                     ind_trab_parcial smallint,
                     salario numeric(10,2),
                     tam_estab_jan smallint,
                     ind_aprendiz smallint,
                     origem_informacao smallint,
                     comp_dec varchar(6),
                     ind_fora_prazo smallint,
                     unid_salario_cod smallint,
                     salario_fixo numeric(10,2)
                     );''')
                    
cursor_conn.execute('''create table tbl_caged_mov
                    (competencia_mov varchar(6) not null,
                     regiao smallint not null,
                     uf smallint not null,
                     municipio varchar(6) not null,
                     cnae_secao varchar(1),
                     cnae_subclasse varchar(7) not null,
                     saldo_movimentacao smallint not null,
                     cbo2002_ocupacao varchar(6),
                     categoria varchar(3),
                     grau_instrucao smallint,
                     idade smallint,
                     horas_contratuais varchar(15),
                     raca_cor smallint,
                     sexo smallint,
                     tipo_empregador smallint,
                     tipo_estabelecimento smallint,
                     tipo_movimentacao smallint,
                     tipo_deficiencia smallint,
                     ind_trab_intermitente smallint,
                     ind_trab_parcial smallint,
                     salario numeric(10,2),
                     tam_estab_jan smallint,
                     ind_aprendiz smallint,
                     origem_informacao smallint,
                     comp_dec varchar(6),
                     ind_fora_prazo smallint,
                     unid_salario_cod smallint,
                     salario_fixo numeric(10,2)
                    );''')
                    
                    
# --- Loading data to Postgres --- #
buffer = StringIO()
caged_exc.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_caged_exc', sep = ';', 
                      columns = ('competencia_mov', 'regiao', 'uf', 'municipio', 'cnae_secao', 
                                 'cnae_subclasse', 'saldo_movimentacao', 'cbo2002_ocupacao',
                                 'categoria', 'grau_instrucao', 'idade', 'horas_contratuais',
                                 'raca_cor', 'sexo', 'tipo_empregador', 'tipo_estabelecimento',
                                 'tipo_movimentacao', 'tipo_deficiencia', 'ind_trab_intermitente',
                                 'ind_trab_parcial', 'salario', 'tam_estab_jan', 'ind_aprendiz',
                                 'origem_informacao', 'comp_dec', 'comp_exc', 'ind_exc',
                                 'ind_fora_prazo', 'unid_salario_cod', 'salario_fixo'))

buffer = StringIO()
caged_for.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_caged_for', sep = ';', 
                      columns = ('competencia_mov', 'regiao', 'uf', 'municipio', 'cnae_secao',
                                 'cnae_subclasse', 'saldo_movimentacao', 'cbo2002_ocupacao',
                                 'categoria', 'grau_instrucao', 'idade', 'horas_contratuais',
                                 'raca_cor', 'sexo', 'tipo_empregador', 'tipo_estabelecimento',
                                 'tipo_movimentacao', 'tipo_deficiencia', 'ind_trab_intermitente',
                                 'ind_trab_parcial', 'salario', 'tam_estab_jan', 'ind_aprendiz',
                                 'origem_informacao', 'comp_dec', 'ind_fora_prazo', 'unid_salario_cod',
                                 'salario_fixo'))

buffer = StringIO()
caged_mov.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_caged_mov', sep = ';',
                      columns = ('competencia_mov', 'regiao', 'uf', 'municipio', 'cnae_secao',
                                 'cnae_subclasse', 'saldo_movimentacao', 'cbo2002_ocupacao',
                                 'categoria', 'grau_instrucao', 'idade', 'horas_contratuais',
                                 'raca_cor', 'sexo', 'tipo_empregador', 'tipo_estabelecimento',
                                 'tipo_movimentacao', 'tipo_deficiencia', 'ind_trab_intermitente',
                                 'ind_trab_parcial', 'salario', 'tam_estab_jan', 'ind_aprendiz',
                                 'origem_informacao', 'comp_dec', 'ind_fora_prazo', 'unid_salario_cod',
                                 'salario_fixo'))

                    
cursor_conn.close()
conn.close()

del buffer, db_credentials, conn, cursor_conn#, caged_exc, caged_for, caged_mov