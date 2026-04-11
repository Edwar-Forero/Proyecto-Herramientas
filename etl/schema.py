# Diccionarios o listas base que servirán para contrastar si después de toda 
# la limpieza, tenemos las columnas necesarias mínimas para hacer analítica.
# Se pueden ir robusteciendo a medida que se descubran las variables claves
# que deben estar en todos los años.

VALID_COLUMNS_NACIMIENTOS = [
    # Ej: "departamento", "municipio", "sexo", "peso", "talla"
]

VALID_COLUMNS_FETALES = [
    # Ej: "departamento", "municipio", "semanas_gestacion", "peso_fetal"
]

VALID_COLUMNS_NO_FETALES = [
    # Ej: "departamento", "municipio", "edad", "sexo", "causa_defuncion"
]
