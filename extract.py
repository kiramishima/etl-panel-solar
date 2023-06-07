import os
import re

def check_files_exist():
    """"
    Para verificar si hay archivos en la carpeta de CSV
    """
    dir_path = "/mnt/d/Workspace/ws_python/etl_irc/csv"
    files = [path for path in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, path))]
    return len(files)

def get_files_name_by_group():
    """"
    Regresa el listado de archivos agrupados por planta
    """
    dir_path = "/mnt/d/Workspace/ws_python/etl_irc/csv"
    files = [os.path.join(dirname, filename) for dirname, _, filenames in os.walk(dir_path) for filename in filenames]

    return files