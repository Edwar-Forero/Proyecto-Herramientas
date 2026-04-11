import pandas as pd
import re

def estandarizar_nombres_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza los nombres de las columnas: minúsculas, cambia espacios por '_'
    y quita caracteres especiales para que sean más estables al procesar.
    """
    # Usar .rename() pero con una lambda compleja o reconstruyendo .columns
    nuevas_cols = []
    for col in df.columns:
        # Convertir a minúsculas y quitar espacios en extremos
        col_new = str(col).strip().lower()
        # Cambiar espacios intermedios por guion bajo
        col_new = re.sub(r'\s+', '_', col_new)
        # Quitar acentos
        col_new = re.sub(r'[áäâà]', 'a', col_new)
        col_new = re.sub(r'[éëêè]', 'e', col_new)
        col_new = re.sub(r'[íïîì]', 'i', col_new)
        col_new = re.sub(r'[óöôò]', 'o', col_new)
        col_new = re.sub(r'[úüûù]', 'u', col_new)
        # Quitar cualquier carácter que no sea alfanumérico o guion bajo
        col_new = re.sub(r'[^a-z0-9_]', '', col_new)
        nuevas_cols.append(col_new)
        
    df.columns = nuevas_cols
    return df

def ajustar_tipos_datos(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte tipos de datos para optimizar en memoria DataFrames grandes.
    'object' -> 'category' (si hay poca variabilidad relativa)
    'float64' -> 'float32'
    'int64' -> 'int32'
    """
    n_filas = len(df)
    
    for col in df.columns:
        dtype = str(df[col].dtype)
        
        # Objetos a categoría
        if dtype == 'object':
            # Convertimos a categoría si hay menos del 40% de valores únicos o máximo 1000 categorías.
            # Esto es vital para "millones de registros"
            n_unicos = df[col].nunique(dropna=False)
            if n_unicos / n_filas < 0.4 or n_unicos < 1000:
                df[col] = df[col].astype('category')
                
        # Downcast de numéricos
        elif dtype.startswith('float'):
            df[col] = pd.to_numeric(df[col], downcast='float')
            
        elif dtype.startswith('int'):
             # Ignorar booleanos o similares
             df[col] = pd.to_numeric(df[col], downcast='integer')
             
    return df