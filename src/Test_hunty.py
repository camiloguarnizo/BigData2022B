# pseudo codigo
# 1. leer archivo json
# 2. carga json a hojas excel 
# 3. extrae datos de google sheet
# 4. cargar datos a BD
# 5. extraer datos BD
# 6. transforma datos BD
# 7. une datos de las dos tablas 


# -*- coding: utf-8 -*-
from google.cloud import storage
from fileinput import filename
from lib2to3.pgen2.pgen import DFAState
from pkgutil import get_data
import numpy as np
import pandas as pd
import os
import json
import requests
from bs4 import BeautifulSoup
from dateutil.parser import  parse
import argparse
parser =argparse.ArgumentParser()
from pathlib import Path
import gspread
import oauth2client
from oauth2client.service_account import ServiceAccountCredentials


def main():
    filename=filename="office_modality.json"
    filename2=filename2="tamaño_empresas.json"

    ruta_excel= "https://docs.google.com/spreadsheets/d/1yGPAiAhRz36LrVHXg3MHB7UkIjPtDDiZFJUgvP5_tY0/edit#gid=471493750"
    nombre_hoja1="main_user_info"
    nombre_hoja2="user_extra_info"

    nombre_columna_borrar="years_experience"
    # leer archivos json
    j1=get_data(filename=filename)
    j2=get_data(filename=filename2)

    ## leer archivo google sheet
    sheet1=get_data_sheet(ruta_excel,nombre_hoja1)
    sheet2=get_data_sheet(ruta_excel,nombre_hoja2)

    ## leer datos BD
    data=sheet1=get_data_sheet(ruta_excel,nombre_hoja1)
    data2=sheet2=get_data_sheet(ruta_excel,nombre_hoja2)

    ## eliminar valores segun condicion
    df_elimina_valor_vacancy=elimina_valor_vacancy(data2)
    
       
    ## crear columna nueva año , de un valor diccionario
    df_crear_columnas_fecha_año=crear_columnas_fecha_año(data2)
    
    ## crear columna nueva mes , de un valor diccionario
    df_crear_columnas_fecha_mes=crear_columnas_fecha_mes(data2)
    
    ## borrar columnas
    df_borrar_columna= borrar_columna(data2, nombre_columna_borrar)

    ##minusculas 
    df_minuscualas_espacio=convertir_minuscula(data)
    
     ## cambia formato fecha campo load_date
    df_cambia_formato_fecha=cambio_formato_fecha(data)
    
     ## combina datos de las dos hojas
    df_cambina_datos=union_datos(data, data2)
    
    
    # guardar info resumen
    df_resumen=get_summary(j1)
    df_resumen2=get_summary(j2)
    
    # guardar info en excel 
    save_data(df_resumen, filename)
    save_data(df_resumen2, filename2)

    # guardar info en google sheet 
    save_data_sheet(df_resumen, filename)
    save_data_sheet(df_resumen2, filename2)

   # guardar info en google sheet 
    save_data_dba(sheet1, nombre_hoja1)
    save_data_dba(sheet2, nombre_hoja2)

#eliminar  datos condicionales
def elimina_valor_vacancy(data2):

    data_sin = data2.drop(data2[(data2['vacancy_area_id'] < 2 ) & (data2['employment_status'] !=0 )].index, inplace=True)
    return data_sin
    



## crear columna nueva de años
def crear_columnas_fecha_año (data2):
    dic=data2['years_experience']
    list_fechas= list()
    n_filas= data2.shape[0] 
    for i in range(0, n_filas):

        str_fecha_year=data2['years_experience'][i]
                   

        try:
            
            val_years= dic['years']
            list_fechas.append(val_years)
        except Exception as e:
        
            list_fechas.append(str_fecha_year)
        continue
    
    data2['years']=list_fechas
    data2['years']=pd.to_datetime(data2['years'], errors='coerce')
 
    return str_fecha_year

## crear columna nueva de meses
def crear_columnas_fecha_mes (data2):
    dic=data2['years_experience']
    list_fechas= list()
    n_filas= data2.shape[0] 
    for i in range(0, n_filas):

        str_fecha_mes=data2['years_experience'][i]
                   

        try:
            
            val_years= dic['months']
            list_fechas.append(val_years)
        except Exception as e:
        
            list_fechas.append(str_fecha_mes)
        continue
    
    data2['months']=list_fechas
    data2['months']=pd.to_datetime(data2['months'], errors='coerce')
 
    return str_fecha_mes

## eliminar columna 
def borrar_columna (data2, nombre_columna_borrar):
    data2= data2.drop([nombre_columna_borrar], axis=1)
        
    return data2


### convertir a minuscula
def convertir_minuscula (data):
    list_flis_name= list()
    n_filas= data.shape[0]
    for i in range(0, n_filas):
        str_name=data['last_name'][i]
        
        try:
            val_data= str_name.lower()
            
            list_flis_name.append(val_data)
        except Exception as e:
            list_flis_name.append(str_name)
        continue
    
    data['last_name']=list_flis_name
    

    return str_name



def cambio_formato_fecha (data):
    col= 'load_date'
    data[col]=pd.to_datetime(data[col], errors='coerce')
    
    return data
 

def union_datos(data,data2):
    data3= pd.merge(data,data2, on='user_id',how='inner' )
    
    return data3
 

### obtener datos json
def get_data(filename):

    gcs_json_path = "gs://prueba_data_engineer_bucket"
    data_dir='data'
    data_dir2 = "raw"
    root_dir = Path(".").resolve().parent
    file_path = os.path.join(gcs_json_path,data_dir,data_dir2,filename)  
    with open(file_path) as f:
        data=json.load(f)
    
    data= pd.DataFrame(data)

    return data


def get_summary(data):
  df_resume=data
  return df_resume

### guardar datos en Excel del bucket 
def save_data(df,filename): 
    out_name='Procesado_.csv' 
    root_dir = Path(".").resolve().parent
    out_path = os.path.join("gs://prueba_data_engineer_bucket", 'data', 'processed', out_name)
    df.to_csv(out_path, encoding='latin-1',sep=';')


    print('Guardando en Big Query')
    ### Guardar tabla en bigquery
    df.to_gbq(destination_table='especializacionbigdatacamilo.llamadas_123',  if_exists='append' )

### guardar datos en google shhet 
def save_data_sheet(df,filename): 
    gc= gspread.service_account(filename='credenciales.json')
    ## crear hoja google sheet 
    hoja_calculo = gc.create('Prueba')

 
    ## obtiene la hoja de excel
    hoja_excel = hoja_calculo.get_worksheet(0)

    ## insertar datos
    hoja_excel.update([df.columns.values.tolist()]+ df.values.tolist())



### leer datos en google sheet 
def get_data_sheet(ruta_excel, nombre_hoja): 
    scope = [ruta_excel]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credenciales.json', scope)
    client = gspread.authorize(creds)
    
    sheet  = client.open(nombre_hoja).sheet1

    data2 = sheet.get_all_values()
    

    return data2

### guardar datos en base de datos por hoja
def save_data_dba(df,nombre_hoja): 
   
    ### Guardar tabla en bigquery
    df.to_gbq(destination_table='especializacionbigdatacamilo'+nombre_hoja,  if_exists='append' )


def extraer_data_bd(nombre_hoja, table_2, especializacionbigdatacamilo):
    query ="""
        SELECT
            *
        FROM {especializacionbigdatacamilo}.{nombre_hoja};
        """
    df = pd.read_gbq(query, "especializacionbigdatacamilo", 'standard')
    print(df)
    data =pd.DataFrame(df)
    return data

if __name__=='__main__':
    main()