import pandas as pd
import re

def parsear_columnas_eliminar(ruta_archivo: str, tipo_estadistica: str) -> list:
    """
    Parsea el archivo colEliminar.md y retorna una lista de columnas a eliminar
    según el tipo de estadística ('No fetales', 'Fetales', 'Nacimientos').
    """
    columnas = []
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            contenido = f.read()
            
        # Buscar la sección usando regex
        patron = re.compile(rf'##\s*{tipo_estadistica}\s*\n(.*?)(?=##|$|\Z)', re.DOTALL | re.IGNORECASE)
        match = patron.search(contenido)
        
        if match:
            lineas = match.group(1).splitlines()
            for linea in lineas:
                col = linea.strip()
                if col and not col.startswith('#'):
                    columnas.append(col)
    except Exception as e:
        print(f"Error al leer {ruta_archivo}: {e}")
        
    return columnas

def eliminar_columnas(df: pd.DataFrame, columnas_a_eliminar: list) -> pd.DataFrame:
    columnas_presentes = [col for col in columnas_a_eliminar if col in df.columns]
    if columnas_presentes:
        df = df.drop(columns=columnas_presentes)
        print(f"Borradas {len(columnas_presentes)} columnas de la lista especificada.")
    else:
        print("Ninguna de las columnas a eliminar estaba presente.")
    return df


def encontrar_duplicados(df: pd.DataFrame, subset=None, n=10):
    print("===== ANÁLISIS DE DUPLICADOS =====")
    
    total = len(df)
    
    # Detectar duplicados (incluye todas las columnas si subset=None)
    duplicados_mask = df.duplicated(subset=subset, keep=False)
    
    total_duplicados = duplicados_mask.sum()
    duplicados_unicos = df.duplicated(subset=subset).sum()
    
    print(f"Total de registros: {total}")
    print(f"Filas duplicadas (incluyendo repetidas): {total_duplicados}")
    print(f"Duplicados únicos a eliminar: {duplicados_unicos}")
    
    return df[duplicados_mask]

def eliminar_duplicados(df: pd.DataFrame, subset=None) -> pd.DataFrame:
    print("===== LIMPIEZA DE DUPLICADOS =====")
    
    num_antes = len(df)
    
    duplicados = df.duplicated().sum()
    
    print(f"Duplicados detectados: {duplicados}")
    
    if duplicados == 0:
        print("No se encontraron duplicados.")
        return df
    
    # Eliminación
    df_limpio = df.drop_duplicates(subset=subset)
    
    num_despues = len(df_limpio)
    eliminados = num_antes - num_despues
    
    print(f"Registros eliminados: {eliminados}")
    print(f"Total final: {num_despues}")
    
    return df_limpio