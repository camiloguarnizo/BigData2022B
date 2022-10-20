
###
# -*- coding: utf-8 -*-

from fileinput import filename
from lib2to3.pgen2.pgen import DFAState
from pkgutil import get_data
import numpy as np
import pandas as pd
import os
from dateutil.parser import  parse
import argparse
parser =argparse.ArgumentParser()
from pathlib import Path
import logging

def main():
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    logging.captureWarnings(True)
    logging.basicConfig(
        level=logging.INFO
        , format=log_fmt,
        filename='data/logs/Etl_llamadas2.log'
    )
    
    filename=filename="llamadas123_julio_2023.csv"
    # leer archivo
    data=get_data(filename=filename)
    
    ## eliminar duplicados
    df_limina_duplicados=elimina_duplicados(data)
    
    ## cambia valores nuelos campo UNIDAD duplicados
    df_cambia_Valores=cambio_valores_nulos(data)
    
    ## cambia formato fecha campo FECHA_INICIO_DESPLAZAMIENTO_MOVIL
    df_cambia_formato_fecha=cambio_formato_fecha(data)
    
    ## cambia formato fecha campo RECEPCION
    df_cambia_formato_fecha2=cambio_formato_fecha_2(data)
    
    ##convierte string a numericos
    df_converint=convertir_numerico(data)
    
    ##minusculas , sin espacio y formato unicode
    df_minuscualas_espacio=convertir_minuscula(data)
    
    ## convertir formato datos UTF-8
    df_formato_Datos=formato_datos(data)
    
    ## quitar espacio en blanco
    df_espacio_blanco=quitar_espacio(data)
    
    ##arreglo caracteres
    df_quita_alfa=arreglo_caracteres(data)
    
    
    # guardar info resumen
    df_resumen=get_summary(data)
    
    
    # guardar info resumen
    tipos_resumen=get_tipos(data)
    
    
    # guardar info
    save_data(df_resumen,tipos_resumen, filename)


def elimina_duplicados(data):
    data_sin = data.drop_duplicates()
    
    return data_sin
    

def cambio_valores_nulos(data):
    data['UNIDAD']= data['UNIDAD'].fillna('SIN_DATO')

    return data 


def cambio_formato_fecha (data):
    col= 'FECHA_INICIO_DESPLAZAMIENTO_MOVIL'
    data[col]=pd.to_datetime(data[col], errors='coerce')
    
    return data


def cambio_formato_fecha_2 (data):
    list_fechas= list()
    n_filas= data.shape[0] 
    for i in range(0, n_filas):

        str_fecha=data['RECEPCION'][i]

        try:
            val_datatime= parse(str_fecha, dayfirst=True, yearfirst=False)
            list_fechas.append(val_datatime)
        except Exception as e:
        
            list_fechas.append(str_fecha)
        continue
    
    data['RECEPCION']=list_fechas
    data['RECEPCION']=pd.to_datetime(data['RECEPCION'], errors='coerce')
 
    return str_fecha



def convertir_numerico(data):
    
    data['EDAD']=data['EDAD'].replace({'SIN_DATO': np.nan}) 
    x= np.nan
    f= lambda x: x if pd.isna(x)== True else int(x)
    f(x)
    data['EDAD'].apply(f)
    return data

def convertir_minuscula (data):
    list_flis_localidad= list()
    n_filas= data.shape[0]
    for i in range(0, n_filas):
        str_localidad=data['LOCALIDAD'][i]
        
        try:
            val_data= str_localidad.lower()
            
            list_flis_localidad.append(val_data)
        except Exception as e:
            list_flis_localidad.append(str_localidad)
        continue
    
    data['LOCALIDAD']=list_flis_localidad
    

    return str_localidad




def quitar_espacio (data):
    list_flis_localidad= list()
    n_filas= data.shape[0]
    for i in range(0, n_filas):
        str_localidad=data['LOCALIDAD'][i]
        
        try:
            val_data= str_localidad.lstrip()
            
            list_flis_localidad.append(val_data)
        except Exception as e:
            list_flis_localidad.append(str_localidad)
        continue
    
    data['LOCALIDAD']=list_flis_localidad
    
    return str_localidad


 
def formato_datos (data):
    list_flis_localidad= list()
    n_filas= data.shape[0]
    for i in range(0, n_filas):
        str_localidad=data['LOCALIDAD'][i]
        
        try:
            val_data= str_localidad.decode("latin-1")
            
            list_flis_localidad.append(val_data)
        except Exception as e:
            list_flis_localidad.append(str_localidad)
        continue
    
    data['LOCALIDAD']=list_flis_localidad
    

    return str_localidad   
     
def arreglo_caracteres (data):
    list_flis_localidad= list()
    n_filas= data.shape[0]
    for i in range(0, n_filas):
        str_localidad=data['LOCALIDAD'][i]
        ##print(data['LOCALIDAD'][i])
        a='engativ '
        b='ciudad bol¡var'
        c='fontib¢n'
        d='antonio nari¤o'
        e='san crist¢bal'
        f='los m rtires'
        g='usa'
        if data['LOCALIDAD'][i]==a:
            list_flis_localidad.append('engativa')
        elif data['LOCALIDAD'][i]==b:
            list_flis_localidad.append('ciudad bolivar')
        elif data['LOCALIDAD'][i]==c:
            list_flis_localidad.append('fontibon')
        elif data['LOCALIDAD'][i]==d:
            list_flis_localidad.append('antonio nariño')
        elif data['LOCALIDAD'][i]==e:
            list_flis_localidad.append('san cristobal')
        elif data['LOCALIDAD'][i]==f:
            list_flis_localidad.append('los martires')
        elif data['LOCALIDAD'][i][:3]==g:
            list_flis_localidad.append('usaquen')
        else:    
            list_flis_localidad.append(str_localidad)
        
    
    data['LOCALIDAD']=list_flis_localidad
    

    return str_localidad   
     


def get_summary(data):
  df_resume=data
  return df_resume

def get_tipos(data):
  tipos_resumen=data.info()
  return tipos_resumen


def get_data(filename):
    logger= logging.getLogger('get_data')
    
    data_dir='data'
    data_dir2 = "raw"
    root_dir = Path(".").resolve().parent
    file_path = os.path.join("gs://cguarnizo_bucket_llamadas123",data_dir,data_dir2,filename)
    ##file_path = os.path.join(root_dir,data_dir,data_dir2,filename)

    data = pd.read_csv(file_path, encoding='latin-1', sep=';')
    
    
    logger.info(f'Reading file:{file_path}')
    logger.info(f'la tabla contiene {data.shape[0]} filas y {data.shape[1]} columnas')
    
    return data
    #print(data.shape)
    
    

def save_data(df,df2,filename): 
    out_name='Limpieza_'+ filename
    root_dir = Path(".").resolve().parent
    out_path = os.path.join("gs://cguarnizo_bucket_llamadas123", 'data', 'processed', out_name)
    out_path2 =os.path.join("gs://cguarnizo_bucket_llamadas123", 'data', 'processed', out_name)
    ##out_path = os.path.join(root_dir, 'data', 'processed', out_name)
    ##out_path2 =os.path.join(root_dir, 'data', 'processed', out_name)
    df.to_csv(out_path, encoding='latin-1',sep=';')
    print('Guardando en Big Query')
    ### Guardar tabla en bigquery
    df.to_gbq(destination_table='especializacionbigdatacamilo.llamadas_123',  if_exists='append' )



if __name__=='__main__':
    main()
