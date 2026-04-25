# Proyecto Herramientas para la gestion de proyectos de tecnologia de la informacion: Sistema de Análisis y Visualización de Estadísticas Vitales en Colombia (2020–2024)


## 👥 Integrantes del Grupo

| Nombre Completo       | Código  | Rol            | Correo Electrónico       |
|-----------------------|---------|----------------|--------------------------|
| Edwar Yamir Forero Blanco        | 2559741  | Colaborador  | [edwar.forero@correounivalle.edu.co](mailto:edwar.forero@correounivalle.edu.co)  |
| Juan Alejandro Urrego            | 2569068  | Colaborador  | [juan.alejandro.urrego@correounivalle.edu.co](mailto:juan.alejandro.urrego@correounivalle.edu.co)   |
| Jhojan Serna Henao               | 2259504  | Colaborador  | [jhojan.serna@correounivalle.edu.co](mailto:jhojan.serna@correounivalle.edu.co)  |
| Kevin Hinojosa Osorio            | 2259470  | Colaborador  | [kevin.hinojosa@correounivalle.edu.co](mailto:kevin.hinojosa@correounivalle.edu.co)|
| Jaider Bermudez Giron            | 2569453  | Colaborador  | [jaider.bermudez@correounivalle.edu.co](mailto:jaider.bermudez@correounivalle.edu.co)|

## Arquitectura del Sistema:
El proyecto Sistema de Análisis y Visualización de Estadísticas Vitales en Colombia (2020–2024) utiliza una arquitectura de tres capas (Three-Tier Architecture), diseñada para separar claramente la gestión de datos, la lógica de negocio y la visualización de la información.

Esta arquitectura permite mantener el sistema organizado, escalable y fácil de mantener, además de facilitar el procesamiento y análisis de grandes volúmenes de datos estadísticos.

### Componentes de la Arquitectura:
1. Capa de datos (Data Layer):
La capa de datos corresponde a la bodega de datos, donde se almacenan las estadísticas vitales procesadas.
Se utiliza Supabase, que proporciona una base de datos PostgreSQL administrada, permitiendo almacenar y consultar los datos de forma eficiente.
Los datos se organizan en tablas que contienen información sobre:

    - nacimientos
    - defunciones fetales
    - defunciones no fetales

    Estos datos provienen de fuentes oficiales y son procesados antes de ser cargados a la base de datos.

2. Capa de procesamiento y lógica (Backend):
El backend está desarrollado en Python, utilizando un enfoque basado en API REST. Su función principal es:
    - realizar consultas a la base de datos
    - procesar los datos solicitados
    - enviar la información al frontend para su visualización

    El backend actúa como intermediario entre la bodega de datos y la interfaz de usuario, garantizando que las consultas se realicen de forma eficiente y estructurada.


3. Capa de presentación (Frontend): 
La capa de presentación está desarrollada con React. Su objetivo es proporcionar una interfaz interactiva para la visualización de estadísticas, mediante dashboards y gráficos que permiten explorar los datos de forma clara e intuitiva.

    Entre las visualizaciones se incluyen:
    - estadísticas por año
    - análisis por departamento
    - comparaciones entre distintos tipos de eventos vitales


## Flujo del Sistema:

    Datasets de estadísticas vitales
            │
            ▼
    Procesamiento y limpieza de datos
    (Jupyter Notebooks + Pandas)
            │
            ▼
    Carga a la bodega de datos
    (Supabase / PostgreSQL)
            │
            ▼
    API Backend
    (Python)
            │
            ▼
    Frontend
    (React Dashboard)


## Pipeline de procesamiento de datos:

El proyecto incluye un pipeline de análisis de datos desarrollado en Jupyter Notebooks, donde se realiza el proceso ETL (Extract, Transform, Load).

Se utilizan tres notebooks principales:

- nacimientos.ipynb

- fetales.ipynb

- no_fetales.ipynb

Cada notebook se encarga de:

1. Cargar los datasets originales
2. Explorar la información
3. Limpiar y transformar los datos
4. Generar estadísticas agregadas
5. Preparar los datos para su almacenamiento en la bodega de datos

Este proceso garantiza que la información almacenada en la base de datos esté estructurada y lista para su análisis.

## Tecnologías Utilizadas:

El proyecto utiliza herramientas modernas orientadas al análisis de datos, desarrollo web y visualización.

Backend
- Python
- FastAPI / Flask
- Pandas
- SQL

Frontend
- React
- JavaScript
- Librerías de visualización (Chart.js / Recharts)

Análisis de datos

- Jupyter Notebook
- Pandas
- Matplotlib / Seaborn

Base de datos
- Supabase (PostgreSQL)

Control de versiones
- Git
- GitHub

Manejo de tareas 
- Trello
- Jira