import pandas as pd
import pymongo
import os

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

    


