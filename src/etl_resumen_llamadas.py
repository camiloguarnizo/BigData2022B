# pseudo codigo
# 1. leer archivo csv
# 2. extraer resumen
# 3. guardar resumen en formato csv

from fileinput import filename
from lib2to3.pgen2.pgen import DFAState
from pkgutil import get_data
import pandas as pd
import os

import argparse
parser =argparse.ArgumentParser()
from pathlib import Path

def main():
    filename=filename="llamadas123_julio_2022.csv"
    # leer archivo
    data=get_data(filename=filename)
    # extrer info
    df_resumen=get_summary(data)
    # guardar info
    save_data(df_resumen, filename)


def save_data(df,filename): 
    out_name='resumen_'+ filename
    root_dir = Path(".").resolve().parent
    out_path = os.path.join(root_dir, 'data', 'processed', 'out_name')

    df.to_csv(out_path)




def get_data(filename):
    data_dir='data'
    data_dir2 = "raw"
    root_dir = Path(".").resolve().parent
    file_path = os.path.join(root_dir,data_dir,data_dir2,filename)

    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    return data
    #print(data.shape)

def get_summary(data):

  dict_resumen = dict()
  for col in data.columns:
    valores_unicos = data[col].unique()
    n_valores = len(valores_unicos)
    dict_resumen[col]= n_valores
    
  df_resume = pd.DataFrame.from_dict(dict_resumen, orient='index')
  df_resume.rename({0: 'Count'}, axis=1, inplace=True) #axis=1 es para qiue pandas se mueva por columnas no por filas

  return df_resume

if __name__=='__main__':
    main()