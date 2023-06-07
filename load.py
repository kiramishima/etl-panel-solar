from pymongo import MongoClient
import os
from airflow.models import Variable

def load_data_to_mongo(ti):
    """ Carga la información a Atlas """
    uri = Variable.get("MONGO_URI")
    print("MongoURI ", uri)
    client =  MongoClient(uri)

    db = client['irc_pp_panels']
    collection = db['panels_info']

    data_dict = ti.xcom_pull('Procesar_Datos', key='data')
    print(data_dict)
    # df.reset_index(inplace=True)
    # data_dict = df.to_dict("records")
    # Insert collection
    collection.insert_many(data_dict)