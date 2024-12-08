import requests
import sqlite3
import xml.etree.ElementTree as xmlET
import logging

def create_db():
    conn=sqlite3.connect('listasOnuOfac.db')
    cursor=conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "PersonasONU" (
        "DataID"	TEXT,
        "Version"	TEXT,
        "PrimerNombre"	TEXT,
        "SegundoNombre"	TEXT,
        "TipoLista"	TEXT,
        "Referencia"	TEXT,
        "FechaLista"	TEXT,
        "Comentarios"	TEXT,
        "Titulo"	TEXT,
        "Designacion"	TEXT,
        "Nacionalidad"	TEXT,
        "TipoLista2"	TEXT,
        "PaisDir"	TEXT,
        "TipoFechaNac"	TEXT,
        "AnioNac"	TEXT,
        "CiudadNac"	TEXT,
        "PrivinciaNac"	TEXT,
        "PaisNac"	TEXT,
        "SortKey"	TEXT,
        "SortKeyLastMod"	TEXT
    )''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "PersonasONUAct" (
            "DataId"	TEXT,
            "FechaActualizacion"	TEXT
    )''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "PersonasONUAlias" (
            "DataId"	TEXT,
            "Calidad"	TEXT,
            "Alias"	TEXT
    )''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "ListaOFAC" (
            "Uid"	TEXT,
            "lastName"	TEXT,
            "sdnType"	TEXT,
            "programList"	TEXT
    )''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "ListaAkaOFAC" (
            "Uid"	TEXT,
            "UidAka"	TEXT,
            "Type"	TEXT,
            "Category"	TEXT,
            "lastName"	TEXT
    )''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "ListaAddressOFAC" (
            "Uid"	TEXT,
            "UidAddr"	TEXT,
            "Address"	TEXT,
            "City"	TEXT,
            "PostalCode"	TEXT,
            "Country"	TEXT
    )''')

    conn.commit()
    conn.close()

def cargarXMLONU(url):
    response=requests.get(url)
    xmlEstructura=xmlET.fromstring(response.content)

    conn=sqlite3.connect('listasOnuOfac.db')
    cursor=conn.cursor()

    cursor.execute("DELETE FROM PersonasONU")
    cursor.execute("DELETE FROM PersonasONUAct")
    cursor.execute("DELETE FROM PersonasONUAlias")
    conn.commit()

    for INDIVIDUAL in xmlEstructura.findall('.//INDIVIDUAL'):
        DataId=INDIVIDUAL.find('DATAID').text if INDIVIDUAL.find('DATAID') is not None else None
        Version=INDIVIDUAL.find('VERSIONNUM').text if INDIVIDUAL.find('VERSIONNUM') is not None else None
        PrimerNombre=INDIVIDUAL.find('FIRST_NAME').text if INDIVIDUAL.find('FIRST_NAME') is not None else None
        SegundoNombre=INDIVIDUAL.find('SECOND_NAME').text if INDIVIDUAL.find('SECOND_NAME') is not None else None
        TipoLista=INDIVIDUAL.find('UN_LIST_TYPE').text if INDIVIDUAL.find('UN_LIST_TYPE') is not None else None
        Referencia=INDIVIDUAL.find('REFERENCE_NUMBER').text if INDIVIDUAL.find('REFERENCE_NUMBER') is not None else None
        FechaLista=INDIVIDUAL.find('LISTED_ON').text if INDIVIDUAL.find('LISTED_ON') is not None else None
        Comentarios=INDIVIDUAL.find('COMMENTS1').text if INDIVIDUAL.find('COMMENTS1') is not None else None
        Titulo=INDIVIDUAL.find('.//TITLE/VALUE').text if INDIVIDUAL.find('.//TITLE/VALUE') is not None else None
        Designacion=INDIVIDUAL.find('.//DESIGNATION/VALUE').text if INDIVIDUAL.find('.//DESIGNATION/VALUE') is not None else None
        Nacionalidad=INDIVIDUAL.find('.//NATIONALITY/VALUE').text if INDIVIDUAL.find('.//NATIONALITY/VALUE') is not None else None
        TipoLista2=INDIVIDUAL.find('.//LIST_TYPE/VALUE').text if INDIVIDUAL.find('.//LIST_TYPE/VALUE') is not None else None
        PaisDir=INDIVIDUAL.find('.//INDIVIDUAL_ADDRESS/COUNTRY').text if INDIVIDUAL.find('.//INDIVIDUAL_ADDRESS/COUNTRY') is not None else None
        TipoFechaNac=INDIVIDUAL.find('.//INDIVIDUAL_DATE_OF_BIRTH/TYPE_OF_DATE').text if INDIVIDUAL.find('.//INDIVIDUAL_DATE_OF_BIRTH/TYPE_OF_DATE') is not None else None
        AnioNac=INDIVIDUAL.find('.//INDIVIDUAL_DATE_OF_BIRTH/YEAR').text if INDIVIDUAL.find('.//INDIVIDUAL_DATE_OF_BIRTH/YEAR') is not None else None
        CiudadNac=INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/CITY').text if INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/CITY') is not None else None
        PrivinciaNac=INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/STATE_PROVINCE').text if INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/STATE_PROVINCE') is not None else None
        PaisNac=INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/COUNTRY').text if INDIVIDUAL.find('.//INDIVIDUAL_PLACE_OF_BIRTH/COUNTRY') is not None else None
        SortKey=INDIVIDUAL.find('.//INDIVIDUAL_DOCUMENT/SORT_KEY').text if INDIVIDUAL.find('.//INDIVIDUAL_DOCUMENT/SORT_KEY') is not None else None
        SortKeyLastMod=INDIVIDUAL.find('.//INDIVIDUAL_DOCUMENT/SORT_KEY_LAST_MOD').text if INDIVIDUAL.find('.//INDIVIDUAL_DOCUMENT/SORT_KEY_LAST_MOD') is not None else None

        cursor.execute("INSERT INTO PersonasONU (DataID,Version,PrimerNombre,SegundoNombre,TipoLista,Referencia,FechaLista,Comentarios,Titulo,Designacion,\
        Nacionalidad,TipoLista2,PaisDir,TipoFechaNac,AnioNac,CiudadNac,PrivinciaNac,PaisNac,SortKey,SortKeyLastMod) \
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (DataId,Version,PrimerNombre,SegundoNombre,TipoLista,Referencia,FechaLista,Comentarios,Titulo,\
        Designacion,Nacionalidad,TipoLista2,PaisDir,TipoFechaNac,AnioNac,CiudadNac,PrivinciaNac,PaisNac,SortKey,SortKeyLastMod))

    for INDIVIDUAL in xmlEstructura.findall('.//INDIVIDUAL'):
        DataId=INDIVIDUAL.find('DATAID').text if INDIVIDUAL.find('DATAID') is not None else None
        FechaActualizacion=INDIVIDUAL.find('.//LAST_DAY_UPDATED/VALUE').text if INDIVIDUAL.find('.//LAST_DAY_UPDATED/VALUE') is not None else None

        cursor.execute("INSERT INTO PersonasONUAct (DataID,FechaActualizacion) VALUES (?,?)", (DataId,FechaActualizacion))

    for INDIVIDUAL in xmlEstructura.findall('.//INDIVIDUAL'):
        DataId=INDIVIDUAL.find('DATAID').text if INDIVIDUAL.find('DATAID') is not None else None
        Calidad=INDIVIDUAL.find('.//INDIVIDUAL_ALIAS/QUALITY').text if INDIVIDUAL.find('.//INDIVIDUAL_ALIAS/QUALITY') is not None else None
        Alias=INDIVIDUAL.find('.//INDIVIDUAL_ALIAS/ALIAS_NAME').text if INDIVIDUAL.find('.//INDIVIDUAL_ALIAS/ALIAS_NAME') is not None else None

        cursor.execute("INSERT INTO PersonasONUAlias (DataID,Calidad,Alias) VALUES (?,?,?)", (DataId,Calidad,Alias))

    conn.commit()
    conn.close()

def cargarXMLOFAC(url):

    response = requests.get(url)

    # Verificar si la solicitud
    if response.status_code == 200:
        print("Archivo XML descargado correctamente.")
        xmlEstructura = xmlET.fromstring(response.content)
        namespaces = {'ns': 'https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML'}

        conn=sqlite3.connect('listasOnuOfac.db')
        cursor=conn.cursor()

        cursor.execute("DELETE FROM ListaOFAC")
        cursor.execute("DELETE FROM ListaAkaOFAC")
        cursor.execute("DELETE FROM ListaAddressOFAC")
        conn.commit()

        i=0
        for sdnEntry in xmlEstructura.findall('.//ns:sdnEntry', namespaces):
            Uid=sdnEntry.find('ns:uid', namespaces).text if sdnEntry.find('ns:uid', namespaces) is not None else None
            lastName=sdnEntry.find('ns:lastName', namespaces).text if sdnEntry.find('ns:lastName', namespaces) is not None else None
            sdnType=sdnEntry.find('ns:sdnType', namespaces).text if sdnEntry.find('ns:sdnType', namespaces) is not None else None
            programList=sdnEntry.find('.//ns:programList/ns:program', namespaces).text if sdnEntry.find('.//ns:programList/ns:program', namespaces) is not None else None

            cursor.execute("INSERT INTO ListaOFAC (Uid,lastName,sdnType,programList) VALUES (?,?,?,?)",\
            (Uid,lastName,sdnType,programList))

            conn.commit()
            print(i)
            i+=1

        i=0
        for sdnEntry in xmlEstructura.findall('.//ns:sdnEntry', namespaces):
            Uid=sdnEntry.find('ns:uid', namespaces).text if sdnEntry.find('ns:uid', namespaces) is not None else None
            i+=1
            for aka in xmlEstructura.findall('.//ns:akaList//ns:aka', namespaces):
                UidAka=aka.find('ns:uid', namespaces).text if aka.find('ns:uid', namespaces) is not None else None
                Type=aka.find('ns:type', namespaces).text if aka.find('ns:type', namespaces) is not None else None
                Category=aka.find('ns:category', namespaces).text if aka.find('ns:category', namespaces) is not None else None
                lastName=aka.find('ns:lastName', namespaces).text if aka.find('ns:lastName', namespaces) is not None else None

                cursor.execute("INSERT INTO ListaAkaOFAC (Uid,UidAka,Type,Category,lastName) VALUES (?,?,?,?,?)",\
                (Uid,UidAka,Type,Category,lastName))

            conn.commit()
            print(i)
            i+=1

        i=0
        for sdnEntry in xmlEstructura.findall('.//ns:sdnEntry', namespaces):
            Uid=sdnEntry.find('ns:uid', namespaces).text if sdnEntry.find('ns:uid', namespaces) is not None else None
            i+=1
            for aka in xmlEstructura.findall('.//ns:addressList//ns:address', namespaces):
                UidAddr=aka.find('ns:uid', namespaces).text if aka.find('ns:uid', namespaces) is not None else None
                Address=aka.find('ns:address1', namespaces).text if aka.find('ns:address1', namespaces) is not None else None
                City=aka.find('ns:city', namespaces).text if aka.find('ns:city', namespaces) is not None else None
                PostalCode=aka.find('ns:postalCode', namespaces).text if aka.find('ns:postalCode', namespaces) is not None else None
                Country=aka.find('ns:country', namespaces).text if aka.find('ns:country', namespaces) is not None else None

                cursor.execute("INSERT INTO ListaAddressOFAC (Uid,UidAddr,Address,City,PostalCode,Country) VALUES (?,?,?,?,?,?)",\
                (Uid,UidAddr,Address,City,PostalCode,Country))

            conn.commit()
            print(i)
            i+=1

        conn.commit()
        conn.close()

def main():
    create_db()
    url1='https://scsanctions.un.org/resources/xml/en/consolidated.xml'
    cargarXMLONU(url1)
    url2='https://www.treasury.gov/ofac/downloads/sdn.xml'
    cargarXMLOFAC(url2)

if __name__ == '__main__':
    main()