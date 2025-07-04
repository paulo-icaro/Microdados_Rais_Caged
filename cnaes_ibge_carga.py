# ======================== #
# === CNAE - DATABASE  === #
# ======================== #

# --- Script by: Paulo Icaro --- #


# ================== #
# === Libraries  === #
# ================== #
import psycopg2 as sql        # For manipulating postgres database
import pandas as pd           # For data manipulations
import os                     # For acessing current o.s path
from io import StringIO       # For increasing data insertion performance



# ================= #
# === Cnae List === #
# ================= #
cnaes_ibge = pd.read_excel(io = os.getcwd()+'/Lista_Cnaes_Atualizada.xlsx',
                           sheet_name = 'Lista Consolidada - Cnaes',
                           header = 0, engine = 'openpyxl',
                           dtype = {'Grupamento':str, 'Seção':str, 'Descrição Seção':str,
                                    'Divisão':str, 'Descrição Divisão':str, 'Grupo':str,
                                    'Descrição Grupo': str, 'Classe':str, 'Descrição Classe':str,
                                    'Subclasse':str, 'Descrição Subclasse':str})


# ==================== #
# === SQL Database === #
# ==================== #

# --------------------------------- #
# --- Creating database dw_cnae --- #
# --------------------------------- #
db_credentials = {'host': 'localhost', 'user' : 'paulo', 'password':'baKih4m4*', 'port':'5432', 'database':'postgres'}

conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()
cursor_conn.execute(query = 'drop database if exists db_cnae;')
cursor_conn.execute(query = 'create database db_cnae;')
cursor_conn.close()
conn.close()


# --------------------------------------------------------------- #
# --- Acessing database db_cnae and inserting tables and data --- #
# --------------------------------------------------------------- #
db_credentials.update({'database':'db_cnae'})
conn = sql.connect(**db_credentials)
conn.autocommit = True
cursor_conn = conn.cursor()

# --- Dropping tables --- #
cursor_conn.execute(query = 'drop table if exists tbl_cnae;')

# --- (Re)Creating tables --- #
cursor_conn.execute(query = '''create table tbl_cnae
                    (grupamento text,
                     secao char(1),
                     desc_secao text,
                     divisao char(2),
                     desc_divisao text,
                     grupo char(3),
                     desc_grupo text,
                     classe char(5),
                     desc_classe text,
                     subclasse char(7),
                     desc_subclasse text
                     );''')

# --- Data loading --- #
buffer = StringIO()
cnaes_ibge.to_csv(path_or_buf = buffer, sep = ';', header = False, index = False)
buffer.seek(0)
cursor_conn.copy_from(buffer, 'tbl_cnae', sep = ';',
                      columns = ('grupamento', 'secao', 'desc_secao',
                                 'divisao', 'desc_divisao', 'grupo', 'desc_grupo',
                                 'classe', 'desc_classe', 'subclasse', 'desc_subclasse'))
cursor_conn.close()
conn.close()
del buffer, db_credentials, conn, cursor_conn #, cnaes_ibge
