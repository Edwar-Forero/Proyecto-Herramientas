"""Constantes de campos de esquemas MongoDB."""

# Campos usados frecuentemente en agregaciones
CAMPOS_NACIMIENTOS = {
    "ano": "ano",
    "sexo": "sexo_desc",
    "departamento": "cod_dpto_desc",
    "nivel_educativo_madre": "niv_edum_desc",
    "regimen_salud": "seg_social_desc",
    "edad_madre": "edad_madre_desc"
}

CAMPOS_MORTALIDAD_FETAL = {
    "ano": "ano",
    "departamento": "cod_dpto_desc",
    "sitio_defuncion": "sit_defun_desc"
}

CAMPOS_MORTALIDAD_NO_FETAL = {
    "ano": "ano",
    "departamento": "cod_dpto_desc",
    "grupo_etario": "gru_ed1_desc",
    "sitio_defuncion": "sit_defun_desc"
}
