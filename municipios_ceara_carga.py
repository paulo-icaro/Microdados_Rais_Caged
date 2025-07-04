# =================================== #
# === CEARA MUNICIPIOS - DATABASE === #
# =================================== #

# --- Script by: Paulo Icaro --- #


# ================== #
# === Libraries  === #
# ================== #
import psycopg2 as sql        # For manipulating postgres database
import pandas as pd           # For data manipulations
import os                     # For acessing current o.s path
from io import StringIO       # For increasing data insertion performance


# =========================== #
# === municipios List === #
# =========================== #
ceara_municipios = pd.read_excel(io = os.getcwd()+'/Lista_Municipios_Brasil.xlsx', sheet_name = 'Ceara', header = 0, engine = 'openpyxl',
                                     dtype = {'Cod. 7 Digitos':str, 'Cod. 6 Digitos':str, 'Municipio':str, 'Municipio - CE':str, 'Regiao':str, 'Escala':int})



# ==================== #
# === SQL Database === #
# ==================== #


# --------------------------------- #
# --- Creating database dw_cnae --- #
# --------------------------------- #
db_credentials = {'host':'localhost', 'user':'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'postgres'}

conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()
cursor_conn.execute(query = 'drop database if exists db_ceara_municipios;')
cursor_conn.execute(query = 'create database db_ceara_municipios;')
cursor_conn.close()
conn.close()


# ------------------------------------------------------------------------- #
# --- Acessing database db_ceara_municipios and inserting tables and data --- #
# ------------------------------------------------------------------------- #
db_credentials.update({'database':'db_ceara_municipios'})
conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()

# --- Dropping tables --- #
cursor_conn.execute(query = 'drop table if exists tbl_ceara_municipios;')

# --- (Re)Creating tables --- #
cursor_conn.execute(query = '''create table tbl_ceara_municipios
                    (cod_7dig char(7),
                     cod_6dig char(6),
                     municipio varchar(25),
                     municipio_ce varchar(30),
                     regiao varchar(30),
                     escala smallint
                     );''')

# --- Data loading --- #
buffer = StringIO()
ceara_municipios.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_ceara_municipios', sep = ';',
                      columns = ('cod_7dig', 'cod_6dig', 'municipio',
                                 'municipio_ce', 'regiao', 'escala'))
cursor_conn.close()
conn.close()
del buffer, db_credentials, conn, cursor_conn #,municipios_ceara                    