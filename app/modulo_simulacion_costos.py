# -*- coding: utf-8 -*-
"""
Created on Wed Sep 22 14:33:58 2021

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
    df_nombrecluster = querySQL( "select * from scac_at1_nombrecluster" , () )
    
    #Dataframe con la ubicacion de las obras
    df_ubicacionobras = querySQL( "select * from SCAC_AV17_UbicacionObrasSCAC" , () )
    
    #Dataframe co la ubicacion de las plantas de concreto
    df_ubicacionplantas = querySQL( "select * from  Coordenadas_Plantas_Concreto", () )
    df_ubicacionplantas = df_ubicacionplantas[['COD_SAP_PLANTA','LATITUD_PLANTA','LONGITUD_PLANTA']]
    df_nombrecluster_aux = df_nombrecluster[(df_nombrecluster['Plantas_Fijas'] == 'Central')&(df_nombrecluster['Activo'] == '1')] #filtro solo las plantas fijas
    df_ubicacionplantas = pd.merge(df_ubicacionplantas, df_nombrecluster_aux, how = 'inner', left_on='COD_SAP_PLANTA', right_on='Centro')
    df_ubicacionplantas = df_ubicacionplantas[['COD_SAP_PLANTA','LATITUD_PLANTA','LONGITUD_PLANTA']]


    #dataframe con la informacion de despacho
    df_despacho = querySQL( "{CALL SCAC_AP10_dataset_servicios_rango (?,?,?)}" , (pais, fechainicio, fechafin) )
    df_despacho['Entrega'] = df_despacho['Entrega'].str.strip()
    df_despacho['VolPartida'] = pd.to_numeric(df_despacho['VolPartida'])
    df_despacho['TiempoCargue'] = pd.to_numeric(df_despacho['TiempoCargue'])
    df_despacho['DistanciaPlantaObraKm'] = pd.to_numeric(df_despacho['DistanciaPlantaObraKm'])
    
    #Dataframe con el consumo de MMPP por despacho
    df = querySQL( "select * from AT51_Z1045_CONSU_TICKET2 where FechaInicio between ? and  ?" , (fechainicio, fechafin) )
    df['Material'] =  df['Material'].str.strip()
    df['Centro'] =  df['Centro'].str.strip()
    df['TipoMaterial'] =  df['TipoMaterial'].str.strip()
    df['UnidadMedida'] =  df['UnidadMedida'].str.strip()
    
    #dataframe de codigos de obra (y plantas) con latitud, longitud y poligono geografico
    df_poligono_geografico = querySQL( "{CALL SCAC_AT15_UbicacionObras_poligonos}" , () )
    df_poligono_geografico['Zona Comercial'] = df_poligono_geografico['Zona Comercial'].str.strip()
    df_poligono_geografico['Obra'] = df_poligono_geografico['Obra'].str.strip()
    
    #dataframe con consumos de combustible
    diesel = pd.read_excel("../datos/Registro Precios y Consumos.xlsx", sheet_name='Combustible')
    
    #Dataframe con el precio de cada MMPP
    df_precios_total =  pd.read_excel("../datos/Registro Precios y Consumos.xlsx", sheet_name='MMPP')
    df_precios_total['Material'] =  df_precios_total['Material'].apply(str)
    df_precios_total['Material'] =  df_precios_total['Material'].str.strip()
    df_precios_total['Centro'] =  df_precios_total['Centro'].str.strip()
    df_precios_total['UnidadMedida'] =  df_precios_total['UnidadMedida'].str.strip()
    df_precios_total['Precio'] =  pd.to_numeric(df_precios_total['Precio'])
    
    #dataframe con capacidades instantaneas, con base en el historico del despacho
    df_capacidad_instantanea = capacidad_instantanea(df_despacho)
    
    #####################################################################################################################
    ############################################  Casos Especiales ######################################################
    
    #filtramos la F049 Y pasamos todo los depachos a la F080
    df_despacho.loc[df_despacho['Centro'] == 'F049', 'Centro'] = 'F080'
    #df_despacho[df_despacho['Centro']=='F080']
    df_despacho = df_despacho[df_despacho['VolEntregado'] > 0]
    
    
    df_nombrecluster = df_nombrecluster[df_nombrecluster['Centro'] != 'F049']
    df_ubicacionplantas = df_ubicacionplantas[df_ubicacionplantas['COD_SAP_PLANTA'] != 'F049']
    
    #####################################################################################################################
        
    return {'nombre_cluster': df_nombrecluster,
            'ubicacion_obras': df_ubicacionobras,
            'ubicacion_plantas': df_ubicacionplantas,
            'despacho': df_despacho,
            'consumos_mmpp': df,
            'poligono_geografico': df_poligono_geografico,
            'combustible': diesel,
            'precios_mmpp':df_precios_total,
            'capacidad': df_capacidad_instantanea }
    


def matriz_despachos_opciones(pais, fechainicio, fechafin):
    

    df_informacion = obtener_info(pais, fechainicio, fechafin)
    
    df_nombrecluster = df_informacion['nombre_cluster']
    df_ubicacionobras =  df_informacion['ubicacion_obras']
    df_ubicacionplantas = df_informacion['ubicacion_plantas']
    df_despacho_filtrado = df_informacion['despacho']
    df = df_informacion['consumos_mmpp']
    df_poligono_geografico = df_informacion['poligono_geografico']
    diesel = df_informacion['combustible']
    df_precios_total = df_informacion['precios_mmpp']
    df_capacidad_instantanea = df_informacion['capacidad']
    
    
    #####################################################################################################################
    ############################################  Costo Transporte y MMPP ###############################################
    
    #Obtengo solo las obras activas para reducir el costo de procesamiento en la matriz de distancias
    df_obrasactivas = pd.DataFrame({'Obra': df_despacho_filtrado['Obra'].unique()}) 
    df_obrasactivas = pd.merge(df_obrasactivas, df_ubicacionobras, how='inner', on ='Obra')
    
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
    
    #costo X galon por planta
    diesel = diesel[(diesel['Fecha']  >= fechainicio ) & (diesel['Fecha']  <= fechafin)]
    #diesel['CostoUnitario'] = diesel['CostoTotal'] / (diesel['Cantidad']/1000)
    diesel['CostoUnitario'] = diesel['Precio']
    
    #kilometros totales
    km_totales = df_despacho_filtrado.groupby(['Planta']).agg({'DistanciaPlantaObraKm':'sum'}).reset_index()
    km_totales = pd.merge(km_totales, df_nombrecluster[['Centro', 'Planta Unica']], how='left', left_on='Planta', right_on='Planta Unica')
    km_totales['km_totales'] = km_totales['DistanciaPlantaObraKm'] * 2
    
    km_totales = km_totales[['Centro', 'km_totales']]
    
    #Radio de cobertura de cada planta
    rango_cobertura = df_despacho_filtrado.groupby(['Centro']).agg({'DistanciaPlantaObraKm':'mean'}).reset_index()
    rango_cobertura.rename(columns={'DistanciaPlantaObraKm':'radio_cobertura', 'Centro':'CentroOpcion'}, inplace = True)
    
    #rendimiento combustible
    diesel_rendimiento = pd.merge(km_totales, diesel, how='left', on='Centro')
    #diesel_rendimiento['galones_por_kilometro'] = (diesel_rendimiento['Cantidad']/1000)/diesel_rendimiento['km_totales']
    diesel_rendimiento['galones_por_kilometro'] = (diesel_rendimiento['Cantidad'])/diesel_rendimiento['km_totales']
    
    #al dataset de despacho le agrego sus centros alternativos
    df_despacho_opciones = df_despacho_filtrado.copy()
    df_despacho_opciones = pd.merge(df_despacho_opciones, df_matriz_km_obra_planta, how='left', on='Obra' )
    df_despacho_opciones = df_despacho_opciones[['Entrega', 'Pedido', 'servicio', 'FechaEntrega', 'Cluster', 'Planta', 'Centro', 'Obra', 'NombreObra', 'HoraEntregaPartida', 'HrReq' ,'VolEntregado','COD_SAP_PLANTA', 'km' , 'latitud', 'longitud', 'LATITUD_PLANTA', 'LONGITUD_PLANTA']]
    df_despacho_opciones.rename(columns={'COD_SAP_PLANTA': 'CentroOpcion'}, inplace=True)
    df_despacho_opciones['latitud'] = pd.to_numeric(df_despacho_opciones['latitud'])
    df_despacho_opciones['longitud'] = pd.to_numeric(df_despacho_opciones['longitud'])
    df_despacho_opciones['LATITUD_PLANTA'] = pd.to_numeric(df_despacho_opciones['LATITUD_PLANTA'])
    df_despacho_opciones['LONGITUD_PLANTA'] = pd.to_numeric(df_despacho_opciones['LONGITUD_PLANTA'])
    #agrego radio de cobertura
    df_despacho_opciones = pd.merge(df_despacho_opciones, rango_cobertura, on='CentroOpcion')
    
    #filtro las opciones que se encuentren dentro del radio de cobertura mas una tolerancia
    df_despacho_opciones = df_despacho_opciones[(df_despacho_opciones['km'] <= df_despacho_opciones['radio_cobertura'] * 1.1) | (df_despacho_opciones['Centro'] == df_despacho_opciones['CentroOpcion'])]
    
    #Join con nombre cluster para dar nombre al centro alterno
    df_despacho_opciones = pd.merge(df_despacho_opciones, df_nombrecluster[['Centro', 'Planta Unica']], how='inner', left_on='CentroOpcion', right_on='Centro').drop("Centro_y",1)
    df_despacho_opciones.rename(columns={'Centro_x': 'Centro'}, inplace=True)
    df_despacho_opciones.rename(columns={'Planta Unica': 'PlantaOpcion'}, inplace=True)
    
    #merge despacho e info de diesel
    df_despacho_opciones = pd.merge(df_despacho_opciones, diesel_rendimiento[['Centro','CostoUnitario', 'galones_por_kilometro']], how='inner', left_on='CentroOpcion', right_on='Centro').drop("Centro_y",1)
    df_despacho_opciones.rename(columns={'Centro_x': 'Centro'}, inplace=True)
    
    #agrego informacion de poligonos geograficos de las obras
    df_despacho_opciones = pd.merge( df_despacho_opciones, df_poligono_geografico[['Obra','Zona Comercial']] , how='left', on='Obra')
    df_despacho_opciones.rename(columns={'Zona Comercial': 'ZonaComercialObra'}, inplace=True)
    
    df_despacho_opciones = pd.merge( df_despacho_opciones, df_poligono_geografico[['Obra','Zona Comercial']] , how='left', left_on='CentroOpcion', right_on='Obra').drop('Obra_y',1)
    df_despacho_opciones.rename(columns={'Obra_x': 'Obra'}, inplace=True)
    
    df_despacho_opciones.rename(columns={'Zona Comercial': 'ZonaComercialPlanta'}, inplace=True)
    
    #costo de transporte por despacho
    df_despacho_opciones['CostoTransporte'] = (df_despacho_opciones['km'] * 2) * df_despacho_opciones['galones_por_kilometro'] * df_despacho_opciones['CostoUnitario']
    
    #Penalidad por zonas comerciales distintas
    df_despacho_opciones['Penalidad_Traspaso_Zona'] =np.where( (df_despacho_opciones['ZonaComercialObra'] != df_despacho_opciones['ZonaComercialPlanta']), df_despacho_opciones['CostoTransporte'] * 3 , 0.0)
    
    #costo de transporte por despacho
    df_despacho_opciones['CostoTransporte_conPenalidad'] =  df_despacho_opciones['CostoTransporte'] + df_despacho_opciones['Penalidad_Traspaso_Zona']
    
    #Version costo transporte
    df_despacho_opciones['VersionCostoTransporte'] = np.where( (df_despacho_opciones['Centro']==df_despacho_opciones['CentroOpcion']), "Real", "Alterna")
    
    #rank de las opciones
    df_despacho_opciones['PuestoOpcionTransporte'] = df_despacho_opciones.groupby('Entrega')['CostoTransporte_conPenalidad'].rank(ascending=True, method='first')
    
    
    #dataframe con el detalle de cada entrega y el material utilizado
    df_mmpp_entrega = df[(df['FechaInicio'] >= fechainicio) & (df['FechaInicio'] <= fechafin) & (df['Material']!= '91')]
    df_mmpp_entrega = df_mmpp_entrega[['Entrega', 'Material', 'CantidadReal', 'UnidadMedida', 'TipoMaterial']]
    
    #filtro los precios del rango de fecha
    df_precios = df_precios_total[(df_precios_total['Fecha'] >= fechainicio) & (df_precios_total['Fecha'] <= fechafin)]
    
    df_precios['TipoMaterial'] = np.select(
        [
            (df_precios['TipoProducto'].str.upper()).str.contains('CEM '),
            (df_precios['TipoProducto'].str.upper()).str.contains('ARENA'),
            (df_precios['TipoProducto'].str.upper()).str.contains('GRAVA'),
            (df_precios['TipoProducto'].str.upper()).str.contains('ISO'),
            (df_precios['TipoProducto'].str.upper()).str.contains('ADICEM')
    
        ],
        [
            'CEM',
            'ARE',
            'GRA',
            'ADI',
            'ADICEM'
        ],
        default = ''
    
    )
    
    #cantidad total de tipo de material por centro
    df_precios_tipomaterial_totales = df_precios.groupby(['Centro', 'TipoMaterial']).agg({'CantidadMaterial':'sum'}).reset_index()
    df_precios_tipomaterial_totales.rename(columns={'CantidadMaterial':'CantidadMaterialTotal'}, inplace = True)
    
    #unir tablas para tener el total de tipo de material por planta
    df_precios = pd.merge(df_precios, df_precios_tipomaterial_totales, on=['Centro', 'TipoMaterial'])
    df_precios['PrecioPonderado'] = df_precios['Precio'] * (df_precios['CantidadMaterial']/df_precios['CantidadMaterialTotal'])
    
    df_precios_tipomaterial = df_precios.groupby(['Centro','TipoMaterial'])['PrecioPonderado'].sum().reset_index()
    
    #se hace un join por cada opcion alterna por la cantidad de materiales usados
    df_costo_mmpp = pd.merge(df_despacho_opciones, df_mmpp_entrega, how='left', on='Entrega' )
    
    #se incluye el precio de cada mmpp
    df_costo_mmpp['CentroOpcion'] = df_costo_mmpp['CentroOpcion'].str.strip()
    df_costo_mmpp = pd.merge( df_costo_mmpp, df_precios[['Material', 'Centro', 'Precio', 'UnidadMedida', 'TipoMaterial']] , how='left', left_on=['CentroOpcion', 'Material'], right_on=['Centro', 'Material']).drop("Centro_y",1)
    df_costo_mmpp.rename(columns={'Centro_x': 'Centro'}, inplace=True)
    
    df_costo_mmpp = pd.merge( df_costo_mmpp, df_precios_tipomaterial, how='left', left_on=['Centro', 'TipoMaterial_x'], right_on=['Centro', 'TipoMaterial'])
    
    df_costo_mmpp['PrecioMaterial'] = np.select(
        [
            df_costo_mmpp['Precio'] > 0
    
        ],
        [
            df_costo_mmpp['Precio']
        ],
        default =  df_costo_mmpp['PrecioPonderado']
    
    )
    
    
    #determino el costo de mmpp por cada opcion de entrega
    df_costo_mmpp['CostoMMPP'] = np.where((df_costo_mmpp['UnidadMedida_x'] != 'TN') | (df_costo_mmpp['UnidadMedida_x'] != 'L'), df_costo_mmpp['CantidadReal'] * (df_costo_mmpp['PrecioMaterial']/1000), (df_costo_mmpp['CantidadReal'] *  df_costo_mmpp['PrecioMaterial']) )
    #copia para retornar datos 
    df_costo_mmpp_copy = df_costo_mmpp.copy()
    df_costo_mmpp = df_costo_mmpp.groupby(['Entrega', 'CentroOpcion']).agg({'CostoMMPP':'sum'}).reset_index()
    
    #merge con la dataframe con todas las opciones
    df_despacho_opciones = pd.merge(df_despacho_opciones, df_costo_mmpp, how='left', left_on=['Entrega','CentroOpcion'], right_on=['Entrega','CentroOpcion'] )
    df_despacho_opciones['CostoMMPP'] = df_despacho_opciones['CostoMMPP'] / df_despacho_opciones['VolEntregado'] 
    df_despacho_opciones['CostoMMPP'] = pd.to_numeric(df_despacho_opciones['CostoMMPP'])
    #rank del costo de MMPP
    df_despacho_opciones['PuestoOpcionMMPP'] = df_despacho_opciones.groupby('Entrega')['CostoMMPP'].rank(ascending=True, method='first')
    
    #Costo de transporte + costo de mmpp
    df_despacho_opciones['CostoProduccion'] = df_despacho_opciones['CostoTransporte'] + (df_despacho_opciones['CostoMMPP'] * df_despacho_opciones['VolEntregado'] )
    df_despacho_opciones['CostoProduccion_Penalidad'] = pd.to_numeric(df_despacho_opciones['CostoTransporte'] + (df_despacho_opciones['CostoMMPP'] * df_despacho_opciones['VolEntregado'] ) + df_despacho_opciones['Penalidad_Traspaso_Zona'])
    
    #rank del costo total
    df_despacho_opciones['PuestoOpcionProduccion'] = df_despacho_opciones.groupby('Entrega')['CostoProduccion_Penalidad'].rank(ascending=True, method='first')
    df_despacho_opciones['Version'] = np.where( (df_despacho_opciones['Planta']==df_despacho_opciones['PlantaOpcion']), "Real", "Alterna")

    return {'opciones':df_despacho_opciones, 'detalle_mmpp': df_costo_mmpp_copy ,'capacidad':df_capacidad_instantanea}

#PARAMETROS QUE VIENEN DEL LLAMADO EXTERIOR AL MODULO, CUANDO TERMINE HAY QUE ELIMINARLOS
pais = 'Colombia'
fechainicio = '2021-01-01'
fechafin = '2021-01-31'
apagar_plantas = ['F006']

df_inputs_simulador = matriz_despachos_opciones(pais, fechainicio, fechafin)

df_opciones = df_inputs_simulador['opciones']
df_capacidad_instantanea = df_inputs_simulador['capacidad']
df_mmpp = df_inputs_simulador['detalle_mmpp']

#arreglo de formatos
df_opciones['FechaEntrega'] = pd.to_datetime(df_opciones['FechaEntrega'])
df_opciones['diaentrega'] = pd.to_numeric(df_opciones['FechaEntrega'].dt.day)

#algoritmo de asignacion

#creo una matriz para ir consolidando los datos de las asignaciones
#df lista de centros unicos
#centro = pd.DataFrame({'Centro':  df_opciones['Centro'].unique() })
centro = df_opciones.groupby(['Cluster','Centro']).size().reset_index()
centro = centro[['Cluster', 'Centro']]
centro['key'] = 1
#df lista de dias del mes ( 1,...,31)
dia = pd.DataFrame({'dia':list(range(1,32))})
dia['key'] = 1
#lista de horas del dia, del 0 al 23
hora = pd.DataFrame({'hora':list(range(0,24))})
hora['key'] = 1
#cross join
df_consolidado = pd.merge(centro, dia, on='key')
df_consolidado= pd.merge(df_consolidado, hora, on='key').drop("key",1)
df_consolidado['vol_registrado'] = 0 
df_consolidado['vol_sobreprogramado'] = 0

df_consolidado = pd.merge(df_consolidado, df_capacidad_instantanea, how='left', on='Centro')
df_consolidado = df_consolidado.to_dict('records')

df_opciones.sort_values(by=['Entrega', 'PuestoOpcionProduccion'], ascending=True, inplace=True)
df_opciones_dict = df_opciones.to_dict('records')
flag = False

despacho_optimo = []

temp_entrega = df_opciones_dict[0]
#recorro la matriz de opciones
for row in df_opciones_dict:
    if ( (flag == False) & (temp_entrega['Entrega'] != row['Entrega']) ):
        #se recorre el diccionario de consolidacion para saber donde ubicar el servicio en SOBREPROGRAMACION
        for row2 in df_consolidado:
            #se busca dia, hora y centro con pacidad instantanea disponible
            if ((row2['dia'] == temp_entrega['diaentrega']) & (row2['hora']== temp_entrega['HoraEntregaPartida']) & (temp_entrega['CentroOpcion'] == row2['Centro'])):
                row2['vol_sobreprogramado'] += temp_entrega['VolEntregado']
                despacho_optimo.append(temp_entrega)
                break
                
        temp_entrega = row
        flag = False
        
        #se recorre el diccionario de consolidacion para saber donde ubicar el servicio
        for row2 in df_consolidado:
            #se busca dia, hora y centro con pacidad instantanea disponible
            if ((row2['dia'] == row['diaentrega']) & (row2['hora']== row['HoraEntregaPartida']) & (row['CentroOpcion'] == row2['Centro'] ) & (row2['vol_registrado'] < row2['capacidad_instantanea'])):
                #el volumen queda registrado en la planta opcion 
                row2['vol_registrado'] += row['VolEntregado']
                #flag es True para indicar que el volumen se asigno
                flag = True
                #como ya se asigno, no se necesita iterar mas sobre el consolidado
                despacho_optimo.append(row)
                break
                
    elif ( (flag == False) & (temp_entrega['Entrega'] == row['Entrega']) ):
        
         #se recorre el diccionario de consolidacion para saber donde ubicar el servicio
        for row2 in df_consolidado:
            #se busca dia, hora y centro con pacidad instantanea disponible
            if ((row2['dia'] == row['diaentrega']) & (row2['hora']== row['HoraEntregaPartida']) & (row['CentroOpcion'] == row2['Centro'] ) & (row2['vol_registrado'] < row2['capacidad_instantanea'])):
                #el volumen queda registrado en la planta opcion 
                row2['vol_registrado'] += row['VolEntregado']
                #flag es True para indicar que el volumen se asigno
                flag = True
                #como ya se asigno, no se necesita iterar mas sobre el consolidado
                despacho_optimo.append(row)
                break


    elif ( (flag == True) & (temp_entrega['Entrega'] != row['Entrega']) ):
        temp_entrega = row
        flag = False
        #se recorre el diccionario de consolidacion para saber donde ubicar el servicio
        for row2 in df_consolidado:
            #se busca dia, hora y centro con pacidad instantanea disponible
            if ((row2['dia'] == row['diaentrega']) & (row2['hora']== row['HoraEntregaPartida']) & (row['CentroOpcion'] == row2['Centro'] ) & (row2['vol_registrado'] < row2['capacidad_instantanea'])):
                #el volumen queda registrado en la planta opcion 
                row2['vol_registrado'] += row['VolEntregado']
                #flag es True para indicar que el volumen se asigno
                flag = True
                #como ya se asigno, no se necesita iterar mas sobre el consolidado
                despacho_optimo.append(row)
                break
 

despacho_optimo = pd.DataFrame.from_dict(despacho_optimo)
df_consolidad_result = pd.DataFrame.from_dict(df_consolidado)

        
writer = pd.ExcelWriter("../datos/simulacion" + "_" + pd.to_datetime("now").strftime("%Y%m%d%H%M%S") + ".xlsx", engine='xlsxwriter')
df_opciones.to_excel( writer, sheet_name="Simulacion" )
df_capacidad_instantanea.to_excel( writer, sheet_name="Capacidad Instantanea" )
df_mmpp.to_excel( writer, sheet_name="MMPP" )
despacho_optimo.to_excel( writer, sheet_name="optimo" )
df_consolidad_result.to_excel( writer, sheet_name="resumen_optimo" )
writer.save()
