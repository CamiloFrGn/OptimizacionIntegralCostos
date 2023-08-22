# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 09:05:08 2021

@author: jsdelgadoc
"""

import numpy as np
import pandas as pd
from math import sin, cos, acos
import modulo_conn_sql as mcq
import modulo_info_aux as mia
from pandas.tseries.offsets import MonthEnd, MonthBegin
import datetime 

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

#PARAMETROS QUE VIENEN DEL LLAMADO EXTERIOR AL MODULO, CUANDO TERMINE HAY QUE ELIMINARLOS
pais = 'Colombia'
fechainicio = '2021-11-30'
meses_historia = 1
estatus_analizar = ['Completada - Cabecera', 'Confirmada - Cabecera', 'En proceso - Cabecera']

#Dataframe con nombre cluster
df_nombrecluster = querySQL( "select * from scac_at1_nombrecluster" , () )

#Dataframe con la ubicacion de las obras
df_ubicacionobras = querySQL( "select * from SCAC_AV17_UbicacionObrasSCAC" , () )

#Dataframe co la ubicacion de las plantas de concreto
df_ubicacionplantas = querySQL( "select * from  Coordenadas_Plantas_Concreto", () )
df_ubicacionplantas = df_ubicacionplantas[['COD_SAP_PLANTA','LATITUD_PLANTA','LONGITUD_PLANTA']]
df_nombrecluster_aux = df_nombrecluster[(df_nombrecluster['Plantas_Fijas'] == 'Central')&(df_nombrecluster['Activo'] == '1')] #filtro solo las plantas fijas
df_ubicacionplantas = pd.merge(df_ubicacionplantas, df_nombrecluster_aux, how = 'inner', left_on='COD_SAP_PLANTA', right_on='Centro')
df_ubicacionplantas = df_ubicacionplantas[['COD_SAP_PLANTA','LATITUD_PLANTA','LONGITUD_PLANTA']]
############################################  Casos Especiales ######################################################
df_ubicacionplantas = df_ubicacionplantas[df_ubicacionplantas['COD_SAP_PLANTA'] != 'F049']


#dataframe con la informacion de la programacion ---------- OJO QUE SOLO EXTRAE LA INFORMACION DE PLANTAS FIJAS ----------
df_despacho = querySQL( "{CALL SCAC_AP19_programacion_hoy_futuro (?,?)}" , (pais, fechainicio) )
df_despacho['VolPartida'] = pd.to_numeric(df_despacho['VolPartida'])
df_despacho['EstatusPedido'] = df_despacho['EstatusPedido'].str.strip()
df_despacho['EstatusPosicion2'] = df_despacho['EstatusPosicion2'].str.strip()

df_despacho = df_despacho[df_despacho['EstatusPedido'].isin(estatus_analizar)]

df_obrasactivas = pd.DataFrame({'Obra': df_despacho['Obra'].unique()}) 
df_obrasactivas = pd.merge(df_obrasactivas, df_ubicacionobras, how='inner', on ='Obra')

#informacion historica auxiliar
df_aux = mia.obtener_info(pais, (pd.to_datetime("now") - MonthEnd(meses_historia) - MonthBegin(1) ).strftime("%Y-%m-%d")  , (pd.to_datetime("now") - MonthEnd(meses_historia)).strftime("%Y-%m-%d") )

#dataframe de codigos de obra (y plantas) con latitud, longitud y poligono geografico
df_poligono_geografico = querySQL( "{CALL SCAC_AT15_UbicacionObras_poligonos}" , () )
df_poligono_geografico['Zona Comercial'] = df_poligono_geografico['Zona Comercial'].str.strip()
df_poligono_geografico['Obra'] = df_poligono_geografico['Obra'].str.strip()

#cross join ubicacion plantas para obtener la distancia de cada obra a cada planta
df_obrasactivas['key'] = 1
df_ubicacionplantas['key'] = 1
df_matriz_km_obra_planta = pd.merge(df_obrasactivas, df_ubicacionplantas, on = 'key' ).drop("key",1)

#calculo la distancia haversiana
distances_km = []
for row in df_matriz_km_obra_planta.itertuples(index=False):
    distances_km.append(
       distancia_haversiana(float(row.latitud), float(row.longitud), float(row.LATITUD_PLANTA), float(row.LONGITUD_PLANTA))
   )

df_matriz_km_obra_planta['km'] = distances_km

#Obtengo el top N de las plantas mas cercanas a cada obra activa
df_matriz_km_obra_planta = df_matriz_km_obra_planta.sort_values(['Obra', 'km', 'latitud', 'longitud', 'LATITUD_PLANTA', 'LONGITUD_PLANTA'], ascending = True).groupby('Obra').head(7)
   
#al dataset de despacho le agrego sus centros alternativos
df_despacho_opciones = df_despacho.copy()
df_despacho_opciones = pd.merge(df_despacho_opciones, df_matriz_km_obra_planta, how='left', on='Obra' )
df_despacho_opciones.rename(columns={'COD_SAP_PLANTA': 'CentroOpcion'}, inplace=True)
df_despacho_opciones['latitud'] = pd.to_numeric(df_despacho_opciones['latitud'])
df_despacho_opciones['longitud'] = pd.to_numeric(df_despacho_opciones['longitud'])
df_despacho_opciones['LATITUD_PLANTA'] = pd.to_numeric(df_despacho_opciones['LATITUD_PLANTA'])
df_despacho_opciones['LONGITUD_PLANTA'] = pd.to_numeric(df_despacho_opciones['LONGITUD_PLANTA'])

#Join con nombre cluster para dar nombre al centro alterno
df_despacho_opciones = pd.merge(df_despacho_opciones, df_nombrecluster[['Centro', 'Planta Unica']], how='inner', left_on='CentroOpcion', right_on='Centro').drop("Centro_y",1)
df_despacho_opciones.rename(columns={'Centro_x': 'Centro'}, inplace=True)
df_despacho_opciones.rename(columns={'Planta Unica': 'PlantaOpcion'}, inplace=True)
    
#union de la informacion auxiliar
df_despacho_opciones = pd.merge(df_despacho_opciones, df_aux, left_on='CentroOpcion', right_on='Centro', how= 'left').drop("Centro_y",1).drop("Planta_y",1)
df_despacho_opciones.rename(columns=({'Centro_x':'Centro', 'Planta_x': 'Planta'}), inplace = True)

#filtro las opciones que se encuentren dentro del radio de cobertura mas una tolerancia
df_despacho_opciones = df_despacho_opciones[(df_despacho_opciones['km'] <= df_despacho_opciones['DistanciaPlantaObraKm'] * 1.1) | (df_despacho_opciones['Centro'] == df_despacho_opciones['CentroOpcion'])]


#agrego informacion de poligonos geograficos de las obras
df_despacho_opciones = pd.merge( df_despacho_opciones, df_poligono_geografico[['Obra','Zona Comercial']] , how='left', on='Obra')
df_despacho_opciones.rename(columns={'Zona Comercial': 'ZonaComercialObra'}, inplace=True)

#y tambien informacion de poligonos de las plantas opciones
df_despacho_opciones = pd.merge( df_despacho_opciones, df_poligono_geografico[['Obra','Zona Comercial']] , how='left', left_on='CentroOpcion', right_on='Obra').drop('Obra_y',1)
df_despacho_opciones.rename(columns={'Obra_x': 'Obra'}, inplace=True)
df_despacho_opciones.rename(columns={'Zona Comercial': 'ZonaComercialPlanta'}, inplace=True)
    
#costo de transporte por despacho
df_despacho_opciones['CostoTransporte'] = (df_despacho_opciones['km'] * 2) * df_despacho_opciones['galones_por_kilometro'] * df_despacho_opciones['Precio_unitario_combustible']

#Penalidad por zonas comerciales distintas
df_despacho_opciones['Penalidad_Traspaso_Zona'] =np.where( (df_despacho_opciones['ZonaComercialObra'] != df_despacho_opciones['ZonaComercialPlanta']), df_despacho_opciones['CostoTransporte'] * 3 , 0.0)

#costo de transporte por despacho
df_despacho_opciones['CostoTransporte_conPenalidad'] =  df_despacho_opciones['CostoTransporte'] + df_despacho_opciones['Penalidad_Traspaso_Zona']

#Version costo transporte
df_despacho_opciones['VersionCostoTransporte'] = np.where( (df_despacho_opciones['Centro']==df_despacho_opciones['CentroOpcion']), "Real", "Alterna")

#rank de las opciones
df_despacho_opciones['PuestoOpcionTransporte'] = df_despacho_opciones.groupby(['Pedido', 'Posicion'])['CostoTransporte_conPenalidad'].rank(ascending=True, method='first')

#Identificador de pedidos con posibilidad de optimizacion
df_pedidos_optimizados = df_despacho_opciones[(df_despacho_opciones['PuestoOpcionTransporte'] == 1)&(df_despacho_opciones['VersionCostoTransporte'] == "Alterna")&(df_despacho_opciones['Planta'] != df_despacho_opciones["PlantaOpcion"])]
df_pedidos_optimizados  = df_pedidos_optimizados [['Pedido', 'Posicion']]
df_pedidos_optimizados['PotencialOptimizacion'] = 1

df_despacho_opciones = pd.merge(df_despacho_opciones, df_pedidos_optimizados, on=['Pedido', 'Posicion'], how='left')
df_despacho_opciones['PotencialOptimizacion'] = df_despacho_opciones['PotencialOptimizacion'].fillna(0)

# GUARDAR EN EXCEL PARA DEPURACIONES    
#writer = pd.ExcelWriter("../datos/Simulacion" + fechainicio + "_" + pd.to_datetime("now").strftime("%Y%m%d%H%M%S") + ".xlsx", engine='xlsxwriter')
writer = pd.ExcelWriter("../datos/Simulacion_hoy.xlsx", engine='xlsxwriter')
df_despacho_opciones.to_excel( writer, sheet_name="Simulacion", index=False )
writer.save()