import sqlite3
import logging
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import pandas as pnds

#Programando ejecucion
def ejecucion():
    print("Inicio el cron")

    nombreProceso='Clasificados'
    archivo=os.path.basename(__file__)
    fecha=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    registros=0

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
        clientesQry="SELECT 'cliente' tabla,UPPER(REPLACE(TRIM(nombre1||nombre2||apellido1||apellido2||apellido_casada),' ','')) nombreCC,nombre1,nombre2,apellido1,apellido2,apellido_casada,UPPER(REPLACE(TRIM(documento),' ','')) documento FROM clientes"
        clientes=pnds.read_sql(clientesQry,conn)
        clientes['nombreCC'] = clientes['nombreCC'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

        #Listas
        listasQry="SELECT 'lista' tabla,UPPER(REPLACE(TRIM(nombre),' ','')) nombreCC,nombre,UPPER(REPLACE(TRIM(documento),' ','')) documento FROM lista_control"
        listas=pnds.read_sql(listasQry,conn)
        listas['nombreCC'] = listas['nombreCC'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

        #Fuente
        fuenteQry="SELECT UPPER(REPLACE(TRIM(nombre),' ','')) nombreCC,nombre,'' Tercer_Nombre,UPPER(REPLACE(TRIM(documento),' ','')) documento FROM fuente"
        fuente=pnds.read_sql(fuenteQry,conn)
        fuente['nombreCC'] = fuente['nombreCC'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)

    except Exception as e:
        logsClas('Error',f'Error en queries: {str(e)}')
        estado=str(e)

    #Busqueda de tabla fuente en tablas clientes y listas
    def clasificar(row):
        if row['nombreCC'] in clientes['nombreCC'].values:
            return 'Cartera de Clientes'
        elif row['nombreCC'] in listas['nombreCC'].values:
            return 'Lista de Control'
        else:
            return 'No se encontro en las BD'

    try:
        fuente['Clasificacion'] = fuente.apply(clasificar, axis=1)

        #Formato y extraccion de Excel
        fuente[['Primer_Nombre','Segundo_Nombre','Primer_Apellido','Segundo_Apellido','Apellido_de_Casada']]=fuente['nombre'].str.split(' ',expand=True,n=4)
        columnasExcel=['nombre','Primer_Nombre','Segundo_Nombre','Tercer_Nombre','Primer_Apellido','Segundo_Apellido','Apellido_de_Casada','Clasificacion']
        fuenteExcel=fuente[columnasExcel].sort_values(by='Clasificacion',ascending=[True])
        fuenteExcel.to_excel('clasificados.xlsx',sheet_name='datos',index=False)
        registros=fuenteExcel.shape[0]
        estado='Ok'
        logsClas('Carga','Se completo la carga de archivo excel')
    except Exception as e:
        logsClas('Error',f'Error en carga de archivo: {str(e)}')
        estado=str(e)

    finally:
        #Crear tabla bitacora si no existe
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS bitacora (
                idFuente INTEGER PRIMARY KEY AUTOINCREMENT,
                nombre VARCHAR(250),
                registros VARCHAR(20),
                estado VARCHAR(200),
                archivo VARCHAR(100),
                fecha VARCHAR(100)
            );
        ''')
        conn.commit()

        #Registro en bitacora
        cursor.execute('''
            INSERT INTO bitacora (nombre,registros,estado,archivo,fecha)
            VALUES(?,?,?,?,?);
            ''',(nombreProceso,registros,estado,archivo,fecha))
        conn.commit()
        conn.close()

        logsClas('Fin','Se completo el proceso')

scheduler = BlockingScheduler()
scheduler.add_job(ejecucion, 'cron', day=1, hour=10, minute=0)
scheduler.start()