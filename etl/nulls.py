import pandas as pd


def _columna_a_texto_tras_fill_numerico(serie: pd.Series, valor_imputacion: str) -> pd.Series:
    """
    Tras fillna(texto) en una columna numérica, pandas suele dejar dtype object
    mezclando floats y str; Parquet no puede serializarlo. Homogeneiza a string.
    """
    def celda_a_texto(v):
        if isinstance(v, str):
            return v
        if pd.isna(v):
            return valor_imputacion
        try:
            fv = float(v)
            return str(int(fv)) if fv == int(fv) else str(v)
        except (TypeError, ValueError):
            return str(v)

    # Forzar object + str de Python: en pandas recientes .astype(str) puede dar
    # dtype 'str' (numpy) y fastparquet falla al serializar; object es estable.
    texto = serie.map(celda_a_texto)
    return pd.Series([str(x) for x in texto], index=texto.index, dtype=object, name=texto.name)


def manejar_nulos(df: pd.DataFrame, estrategia: dict) -> pd.DataFrame:
    """
    Maneja nulos (NaN) basado en un diccionario de estrategias puestas por columna.
    
    Ejemplo de `estrategia`:
    {
        'columna_a': {'metodo': 'drop'},
        'columna_b': {'metodo': 'fill', 'valor': 0},
        'columna_c': {'metodo': 'fill_mean'},
        'columna_d': {'metodo': 'fill_mode'}
    }
    """
    for col, conf in estrategia.items():
        if col not in df.columns:
            continue
            
        metodo = conf.get('metodo')

        # Estrategia: eliminar columnas
        """ if metodo == 'drop_cols':
            cols = conf.get('columnas', [])
            print(f"Eliminando columnas: {cols}")
            df = df.drop(columns=[c for c in cols if c in df.columns])
            continue
        # Si la columna no existe, saltar
        if col not in df.columns:
            continue
        print(f"Procesando columna: {col} | método: {metodo}")
        """
        # Estrategia: eliminar filas donde haya NaN en esa columna, son columnas correlacionadas 
        """ if metodo == "drop_rows":
            cols = conf.get("columnas", [])
            print(f"Eliminando filas con nulos en: {cols}")
            df = df.dropna(subset=cols)
            continue """
        
        
        # Estrategia: eliminar filas donde haya NaN en esa columna
        if metodo == 'drop':
            df = df.dropna(subset=[col])
            
        # Estrategia: llenar con un valor constante
        elif metodo == 'fill':
            valor = conf.get('valor')
            orig_dtype = df[col].dtype
            df[col] = df[col].fillna(valor)
            # Homogeneizar si el relleno es texto y la columna era numérica o object
            # (p. ej. idclasadmi leído como object mezcla floats + 'DESCONOCIDO' → fastparquet falla).
            if isinstance(valor, str) and (
                pd.api.types.is_numeric_dtype(orig_dtype)
                or pd.api.types.is_object_dtype(orig_dtype)
            ):
                df[col] = _columna_a_texto_tras_fill_numerico(df[col], valor)
            
        # Estrategia: llenar con el promedio (solo si es numérica)
        elif metodo == 'fill_mean':
            if pd.api.types.is_numeric_dtype(df[col]):
                promedio = df[col].mean()
                df[col] = df[col].fillna(promedio)
                
        # Estrategia: llenar con la moda
        elif metodo == 'fill_mode':
            if not df[col].mode().empty:
                moda = df[col].mode()[0]
                df[col] = df[col].fillna(moda)
                
    return df

def analizar_nulos(df: pd.DataFrame):
    print("===== ANÁLISIS DE NULOS =====")
    
    total = len(df)
    
    # Conteo de nulos por columna
    nulos = df.isnull().sum()
    
    # Porcentaje
    porcentaje = (nulos / total) * 100
    
    resumen = pd.DataFrame({
        "nulos": nulos,
        "porcentaje (%)": porcentaje
    }).sort_values(by="nulos", ascending=False)
    
    # Filtrar solo columnas con nulos
    resumen = resumen[resumen["nulos"] > 0]
    
    print(f"Total de registros: {total}")
    print(f"Columnas con nulos: {len(resumen)}\n")
    
    display(resumen)
    
    return resumen
