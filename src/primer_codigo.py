import numpy as np
import random
import argparse
import pandas as pd

def calcular_min_max(lista_numeros, verbose=1):
    ''' 
    Retorna los valores Minimo y Maximo de una lista de numeros
    Argumentos:
        lista_numeros:type list
    '''
    min_value= min(lista_numeros)
    max_value=max(lista_numeros)
    
    
    if verbose == True:
        print('Valor Minimo:', min_value)
        print('Valor Maximo:',max_value )
    else: 
        pass
    return min_value, max_value

def calcular_suma(lista_numeros, verbose=1):
    '''
    Retorna la suma de lista de numeros 
    '''
    lista=lista_numeros
    suma=0
    for lista_num in lista:
        suma +=lista_num
    
    if verbose == True:
        print('la suma total es:',suma)
    else: 
        pass
      
    return suma

def calcula_valores_centrales(lista_numeros, verbose=1):
    """ Calcula la media y desviacion estandar de una lista

    Args:
        lista_numeros (list): lista con valores numericos
        verbose (bool, optional): para decidir si imprime el resultado
        . Defaults to True.

    Returns:
        tupla: (media, dev_std)
    """
    
    media= np.mean(lista_numeros)
    dev_std=np.std(lista_numeros)
    
    if verbose == True:
        print('Valor Media:',media)
        print('Valor Desviacion:',dev_std)
    else: 
        pass
    return media, dev_std
    
def calcular_valores (lista_numeros, verbose=1):
    """Retorna valores de suma, minimo valor , maximo valor , media y varianza
    numeros lista

    Args:
        lista_numeros (_type_): _description_
        verbose (bool, optional): _description_. Defaults to True.

    Returns:
        _type_: _description_
    """
    suma = calcular_suma(lista_numeros)
    min_val, max_val= calcular_min_max(lista_numeros)
    media, dev_std= calcula_valores_centrales(lista_numeros)
    
    return suma, min_val, max_val, media , dev_std



def main ():
    parser =argparse.ArgumentParser()
    parser.add_argument("--verbose"
                        , type=int, default=1,
                        help='para imprimir en pantalla')
    args =parser.parse_args()
    random_numbers = random.sample(range(15), 8)
    print('lista',random_numbers)
    calcular_valores(lista_numeros=random_numbers ,  verbose=args.verbose) 

if __name__=='__main__':    
    main()     