import sqlite3
import logging
import os
import schedule
from datetime import datetime
import pandas as pnds
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import re

nombreProceso='Clasificados'
archivo=os.path.basename(__file__)
fecha=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
registros=0

spark = SparkSession.builder.appName("Clasificaciones").getOrCreate()
os.environ['PYSPARK_PYTHON']='C:/Users/Diego/AppData/Local/Programs/Python/Python313/python.exe'

#Configurando logging
logging.basicConfig(filename='logs/clasificados.log', level=logging.INFO, format='%(asctime)s - %(message)s')
def logsClas(status,message):
    logging.info(f"{status}:{message}")

#log
logsClas('Inicio','Inicio del ETL para clasificar personas')

#Excel
arcExcel='fuente.xlsx'
fuenteEx=pnds.read_excel(arcExcel)

#Conectar a DB
dataBase='ev_ing_dat.db'
conn=sqlite3.connect(dataBase)
cursor=conn.cursor()

try:
    #Crear tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fuente (
            idFuente INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre VARCHAR(250),
            documento VARCHAR(20)
        );
    ''')

    #Limpiar tabla fuente
    fuenteDltQry="DELETE FROM fuente"
    cursor.execute(fuenteDltQry)
    conn.commit()

    seqDltQry="DELETE FROM sqlite_sequence WHERE name = 'fuente'"
    cursor.execute(seqDltQry)
    conn.commit()

    #Cargar excel en tabla
    for index, row in fuenteEx.iterrows():
        cursor.execute('''
        INSERT INTO fuente (nombre,documento)
        VALUES(?,?);
        ''',(row['Nombre'],row['documento']))
    conn.commit()

except Exception as e:
    logsClas('Error',f'Error en creacion y carga de tabla: {str(e)}')
    estado=str(e)

try:
    #Clientes
    clientesQry="SELECT UPPER(REPLACE(TRIM(REPLACE(nombre1||nombre2||apellido1||apellido2||apellido_casada, '[^a-zA-Z0-9]', '')),' ','')) nombreCC,\
    nombre1,nombre2,apellido1,apellido2,apellido_casada,UPPER(REPLACE(TRIM(documento),' ','')) documento FROM clientes"
    cursor.execute(clientesQry)
    clientes = cursor.fetchall()
    print("clientes")

    #Listas
    listasQry="SELECT UPPER(REPLACE(TRIM(REPLACE(nombre, '[^a-zA-Z0-9]', '')),' ','')) nombreCC,nombre,\
        UPPER(REPLACE(TRIM(documento),' ','')) documento FROM lista_control"
    cursor.execute(listasQry)
    listas = cursor.fetchall()

    #Fuente
    fuenteQry="SELECT UPPER(REPLACE(TRIM(REPLACE(nombre, '[^a-zA-Z0-9]', '')),' ','')) nombreCC,\
        nombre,'' Tercer_Nombre,UPPER(REPLACE(TRIM(documento),' ','')) documento FROM fuente"
    cursor.execute(fuenteQry)
    fuente = cursor.fetchall()

except Exception as e:
    logsClas('Error',f'Error en queries: {str(e)}')
    estado=str(e)

try:
    #Creando data frame de las queries
    clientesSprk=spark.createDataFrame(clientes,["nombreCC","documento"])
    listasSprk=spark.createDataFrame(listas,["nombreCC","documento"])
    fuenteSprk=spark.createDataFrame(fuente,["nombreCC","documento","nombre"])
except Exception as e:
    logsClas('Error',f'Error en creacion de data frames: {str(e)}')
    estado=str(e)

#Buequeda de coincidencias
clasificacionFinal = fuenteSprk.join(clientesSprk, (fuenteSprk["nombreCC"] == clientesSprk["nombreCC"]) &\
(fuenteSprk["documento"] == clientesSprk["documento"]), "left_outer")\
.join(listasSprk, (fuenteSprk["nombreCC"] == listasSprk["nombreCC"]) &\
(fuenteSprk["documento"] == listasSprk["documento"]), "left_outer")

#Agregando columna de resultados
clasificacionFinal=clasificacionFinal.withColumn(
    "Clasificacion",
    F.when(
        (clientesSprk["nombreCC"].isNotNull()) | (listasSprk["nombreCC"].isNotNull()),  # Si alguna columna 'nombreCC' no es nula
        "Encontrado"
    ).otherwise("No encontrado") 
)

#Formato y extraccion de Excel
clasificacionFinal = clasificacionFinal.withColumn(
    'Primer_Nombre', F.split(clasificacionFinal['nombre'], ' ').getItem(0)
).withColumn(
    'Segundo_Nombre', F.split(clasificacionFinal['nombre'], ' ').getItem(1)
).withColumn(
    'Primer_Apellido', F.split(clasificacionFinal['nombre'], ' ').getItem(2)
).withColumn(
    'Segundo_Apellido', F.split(clasificacionFinal['nombre'], ' ').getItem(3)
).withColumn(
    'Apellido_de_Casada', F.split(clasificacionFinal['nombre'], ' ').getItem(4)
).withColumn(
    'Tercer_Nombre', F.lit('')
)

columnasExcel=['nombre','Primer_Nombre','Segundo_Nombre','Tercer_Nombre','Primer_Apellido','Segundo_Apellido','Apellido_de_Casada','Clasificacion']
fuenteExcel= clasificacionFinal.select(columnasExcel).orderBy('Clasificacion', ascending=True)
#fuenteExcel=fuenteExcel.dropna()
fuentePnds=fuenteExcel.toPandas()