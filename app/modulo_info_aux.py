# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 14:28:04 2021

@author: jsdelgadoc
"""

import numpy as np
import pandas as pd
from math import sin, cos, acos
import modulo_conn_sql as mcq

#Query BD SQL-Server Cemex
def querySQL(query, parametros):
    #Conectar con base sql y ejecutar consulta
    cursor = conectarSQL()
    try:
        cursor.execute(query, parametros)
        #obtener nombre de columnas
        names = [ x[0] for x in cursor.description]
        
        #Reunir todos los resultado en rows
        rows = cursor.fetchall()
        resultadoSQL = []
            
        #Hacer un array con los resultados
        while rows:
            resultadoSQL.append(rows)
            if cursor.nextset():
                rows = cursor.fetchall()
            else:
                rows = None
                
        #Redimensionar el array para que quede en dos dimensiones
        resultadoSQL = np.array(resultadoSQL)
        resultadoSQL = np.reshape(resultadoSQL, (resultadoSQL.shape[1], resultadoSQL.shape[2]) )
    finally:
            if cursor is not None:
                cursor.close()
    return pd.DataFrame(resultadoSQL, columns = names)

#SQL Methods to get operation data
def conectarSQL():
    conn = mcq.ConexionSQL()
    cursor = conn.getCursor()
    return cursor


def distancia_haversiana(latitud_obra, longitud_obra, latitud_planta, longitud_planta):

    km = (acos(
    (sin(latitud_obra*0.01745329252)) * 
    (sin(latitud_planta*0.01745329252)) + 
    cos(latitud_obra*0.01745329252) * 
    cos(latitud_planta*0.01745329252) *
    cos(longitud_planta*0.01745329252 - longitud_obra*0.01745329252))*6371)*1.41
    
    return km

def capacidad_instantanea(df):
    #filtro los datos de tiempo de cargue, para filtar atipicos
    df = df[(df['TiempoCargue'] > 2) & (df['TiempoCargue'] < 30)]
    #calculo cuantos m3 se fabrican por minuto
    df['df_m3_min'] = df['VolPartida'] / df['TiempoCargue']
    #obtengo la mediana para evitar datos atipicos
    df_capacidad_instantanea = df.groupby(['Centro']).agg({'df_m3_min': 'median'}).reset_index()
    #multiplico el resultado por 60 (minutos en una hora)
    df_capacidad_instantanea ['capacidad_instantanea'] = df_capacidad_instantanea['df_m3_min'] * 60
    return df_capacidad_instantanea 

def obtener_info(pais, fechainicio, fechafin):
    
    #####################################################################################################################
    ############################################  EXTRACCION DE INFORMACION #############################################
    #####################################################################################################################
            
    #Dataframe con nombre cluster
    df_nombrecluster = querySQL( "select * from scac_at1_nombrecluster where Activo = 1" , () )

    #dataframe con la informacion de despacho
    df_despacho = querySQL( "{CALL SCAC_AP10_dataset_servicios_rango (?,?,?)}" , (pais, fechainicio, fechafin) )
    df_despacho['Entrega'] = df_despacho['Entrega'].str.strip()
    df_despacho['VolPartida'] = pd.to_numeric(df_despacho['VolPartida'])
    df_despacho['TiempoCargue'] = pd.to_numeric(df_despacho['TiempoCargue'])
    df_despacho['DistanciaPlantaObraKm'] = pd.to_numeric(df_despacho['DistanciaPlantaObraKm'])
    
    #Radio de cobertura de cada planta
    rango_cobertura = df_despacho.groupby(['Centro']).agg({'DistanciaPlantaObraKm':'mean'}).reset_index()
    #rango_cobertura.rename(columns={'DistanciaPlantaObraKm':'radio_cobertura', 'Centro':'CentroOpcion'}, inplace = True)
    
    #kilometros totales
    km_totales = df_despacho.groupby(['Planta']).agg({'DistanciaPlantaObraKm':'sum'}).reset_index()
    km_totales = pd.merge(km_totales, df_nombrecluster[['Centro', 'Planta Unica']], how='left', left_on='Planta', right_on='Planta Unica')
    km_totales['km_totales'] = km_totales['DistanciaPlantaObraKm'] * 2
    km_totales = km_totales[['Planta','Centro', 'km_totales']]
    
    #dataframe con consumos de combustible
    diesel = pd.read_excel("../datos/Registro Precios y Consumos.xlsx", sheet_name='Combustible')
    diesel = diesel[(diesel['Fecha']  >= fechainicio ) & (diesel['Fecha']  <= fechafin)]
    diesel.rename(columns = {'Precio': 'Precio_unitario_combustible'}, inplace = True)
    #rendimiento combustible
    diesel_rendimiento = pd.merge(km_totales, diesel, how='left', on='Centro')
    diesel_rendimiento['galones_por_kilometro'] = (diesel_rendimiento['Cantidad'])/diesel_rendimiento['km_totales']
    diesel_rendimiento = diesel_rendimiento[['Centro', 'galones_por_kilometro', 'Precio_unitario_combustible', 'Cantidad']]
    
    df_result = km_totales
    df_result = pd.merge(df_result, rango_cobertura, on = 'Centro', how='left')
    df_result = pd.merge(df_result, diesel_rendimiento, on = 'Centro', how='left')
    
    return df_result

#dias_historia = 10
#pais = 'Colombia'

#df = obtener_info('Colombia',  (pd.to_datetime("now") - datetime.timedelta(dias_historia) - MonthBegin(1) ).strftime("%Y-%m-%d")  ,  pd.to_datetime("now").strftime("%Y-%m-%d")  )


