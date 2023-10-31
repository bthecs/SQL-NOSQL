import pandas as pd
import pymongo
import os
from utils import *

def read_sql():
    portin_dir = os.path.dirname("BASE PORT IN/")
    portout_dir = os.path.dirname("BASE PORT OUT/")
    sql_in = [i for i in os.listdir(portin_dir) if i.endswith(".db")]
    sql_out = [i for i in os.listdir(portout_dir) if i.endswith(".db")]
    lista = []
    lista.extend(sql_in)
    lista.extend(sql_out)
    df_final = pd.DataFrame()
    for i in lista:
        if i in sql_in:
            df = pd.read_sql('SELECT * FROM clients', 'sqlite:///BASE PORT IN/' + i)
        elif i in sql_out:
            df = pd.read_sql('SELECT * FROM clients', 'sqlite:///BASE PORT OUT/' + i)
        df_final = pd.concat([df_final, df])
    
    #eliminar columnas date_port_in y date_port_out
    df_final = df_final.drop(columns=['date_port_in', 'date_port_out'])
    return df_final

    

def create_dataframe_provinces(df):
    area_code = pd.read_excel('area_code.xlsx')
                    
    code_area = {}
    for index, row in area_code.iterrows():
        code_area[str(row['CÓDIGO DE AREA'])] = [row['PROVINCIA']]

    df['province'] = None
    df['line'].astype(str)

    df['province'] = df['line'].apply(lambda x: code_area[x[:2]][0] if x[:2] in code_area else (code_area[x[:3]][0] if x[:3] in code_area else (code_area[x[:4]][0] if x[:4] in code_area else '')))

    df = df.drop_duplicates(subset=['line'], keep='first')

    #crear diferentes dataframes segun su provincia
    df_dict = {}
    for province, group in df.groupby('province'):
        df_dict[province] = group.copy()

    for province, df in df_dict.items():
        df.to_sql('clients', 'sqlite:///provinces/' + province + '.db', if_exists='replace')


def create_collection():
    # crear una base de datos en puerto 27018
    client = pymongo.MongoClient('localhost', 27017)
    db = client['bases']
    
    # leer sql y crear colecciones
    sql = [i for i in os.listdir('provinces/') if i.endswith(".db")]
    #crear colecciones con las bases de datos sql
    for db_file in sql:
        collection_name = db_file.split(".")[0]
        # validar si collection_name existe en el diccionario provinces
        if collection_name in provinces:
            # si existe crear colleccion con el valor de la key de provinces
            collection_value = provinces[collection_name]
            db.create_collection(collection_value)

        else:
            print(db_file + " does not exist")
            continue
            


