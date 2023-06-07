import pandas as pd
import numpy as np
import datetime as dt

def get_df(paths, **kwargs):
    
    ''' Leé multiples datos y los mete en un solo Dataframe '''
    
    if len(paths)==0:
        raise ValueError("there must be at least one valid path")
        
    df = pd.read_csv(paths[0], **kwargs)
    for path in paths[1:]:
        df2 = pd.read_csv(path, **kwargs)
        df = pd.concat([df, df2], ignore_index=True)
        
    return df

def load_data_to_pandas(ti):
    """ Carga toda la información al Dataframe """
    dir_path = "/mnt/d/Workspace/ws_python/etl_irc/csv"
    # Cargamos la información de todas las plantas y sensores
    plant = get_df(["{}/Plant_{}_Generation_Data.csv".format(dir_path, i) for i in range(1,3)], parse_dates=["DATE_TIME"])
    weather = get_df(["{}/Plant_{}_Weather_Sensor_Data.csv".format(dir_path, i) for i in range(1,3)], parse_dates=["DATE_TIME"])

    df = plant.merge(weather, on=["DATE_TIME", "PLANT_ID"], suffixes=("_GENERATION", "_WEATHER"))
    df.reset_index(inplace=True)
    print(df.to_dict("records"))
    ti.xcom_push(key='data', value=df.to_dict("records"))