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
    );

    CREATE TABLE IF NOT EXISTS "PersonasONUAct" (
        "DataId"	TEXT,
        "FechaActualizacion"	TEXT
    );

    CREATE TABLE IF NOT EXISTS "PersonasONUAlias" (
        "DataId"	TEXT,
        "Calidad"	TEXT,
        "Alias"	TEXT
    );

    CREATE TABLE IF NOT EXISTS "ListaOFAC" (
        "Uid"	TEXT,
        "lastName"	TEXT,
        "sdnType"	TEXT,
        "programList"	TEXT
    );

    CREATE TABLE IF NOT EXISTS "ListaAkaOFAC" (
        "Uid"	TEXT,
        "UidAka"	TEXT,
        "Type"	TEXT,
        "Category"	TEXT,
        "lastName"	TEXT
    );

    CREATE TABLE IF NOT EXISTS "ListaAddressOFAC" (
        "Uid"	TEXT,
        "UidAddr"	TEXT,
        "Address"	TEXT,
        "City"	TEXT,
        "PostalCode"	TEXT,
        "Country"	TEXT
    );