# Diccionarios o listas base que servirán para contrastar si después de toda
# la limpieza, tenemos las columnas necesarias mínimas para hacer analítica.
# Se pueden ir robusteciendo a medida que se descubran las variables claves
# que deben estar en todos los años.

VALID_COLUMNS_NACIMIENTOS = ["ano",
                            "area_res",
                            "areanac",
                            "aten_par",
                            "cod_dpto",
                            "cod_munic",
                            "codmunre",
                            "codpres",
                            "codptore",
                            "edad_madre",
                            "edad_padre",     
                            "est_civm",     
                            "idclasadmi",     
                            "idfactorrh",     
                            "idhemoclas",     
                            "mes",     
                            "mul_parto",     
                            "niv_edum",     
                            "niv_edup",     
                            "numconsul",     
                            "peso_nac",     
                            "profesion",     
                            "seg_social",     
                            "sexo",     
                            "sit_parto",     
                            "t_ges",     
                            "talla_nac",    
                            "tipo_parto", 
                            "n_hijosv" , 
                            "tipo_evento"
                             ]

VALID_COLUMNS_FETALES = ["a_defun",     
                         "ano",     
                         "area_res",     
                         "asis_med",     
                         "cau_homol",     
                         "cod_dpto",     
                         "cod_munic",     
                         "codmunre",     
                         "codptore",     
                         "edad_madre",     
                         "est_civm",     
                         "mes",     
                         "mu_parto",     
                         "n_hijosm",     
                         "n_hijosv",     
                         "niv_edum",     
                         "peso_nac",     
                         "sexo",     
                         "sit_defun",     
                         "t_ges",     
                         "t_parto",     
                         "tipo_defun",     
                         "tipo_emb",
                         "tipo_evento",
                         ]

VALID_COLUMNS_NO_FETALES = ['a_defun',
                            'ano',
                            'area_res',
                            'asis_med',
                            'cod_dpto',
                            'cod_munic',
                            'codmunre',
                            'codptore',
                            'est_civil',
                            'gru_ed1',
                            'gru_ed2',
                            'idpertet',
                            'mes',
                            'muerteporo',
                            'nivel_edu',
                            'seg_social',
                            'sexo',
                            'sit_defun',
                            'tipo_defun',
                            'ultcurfal',
                            'tipo_evento',                            
                            ]


def validar_schema(df, schema):
    faltantes = []
    tipos_incorrectos = []
    correctas = []

    for columna, tipo in schema.items():
        if columna not in df.columns:
            faltantes.append(columna)
        elif str(df[columna].dtype) != tipo:
            tipos_incorrectos.append((columna, tipo, str(df[columna].dtype)))
        else:
            correctas.append(columna)

    extras = [c for c in df.columns if c not in schema]

    print("\n=== RESULTADO VALIDACIÓN ===")
    print(f"✔ Columnas correctas: {len(correctas)}")

    if faltantes:
        print(f"\n❌ Faltantes ({len(faltantes)}): {faltantes}")

    if tipos_incorrectos:
        print(f"\n⚠ Tipos incorrectos:")
        for col, esp, enc in tipos_incorrectos:
            print(f"  {col}: esperado {esp}, encontrado {enc}")

    if extras:
        print(f"\n➕ Columnas extra ({len(extras)}): {extras}")
