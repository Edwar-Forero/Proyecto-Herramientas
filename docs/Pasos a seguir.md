Quiero que desarrolles el sistema COMPLETO (frontend + backend) de mi proyecto de Business Intelligence enfocado en estadísticas vitales de Colombia (2020–2024), usando la arquitectura actual del proyecto SIN modificarla ni reorganizarla.

Debes actuar como un arquitecto senior especializado en:

* dashboards analíticos
* Business Intelligence
* FastAPI
* MongoDB
* sistemas escalables
* visualización de datos
* frontend moderno con Next.js

========================
ARQUITECTURA ACTUAL
===================

IMPORTANTE:
NO debes crear una nueva arquitectura.
NO debes convertir el proyecto en monorepo.
NO debes mover carpetas existentes.
Debes trabajar SOBRE la arquitectura ya definida.

Arquitectura actual:

C:.
├── backend
│   └── app
│       ├── database
│       ├── models
│       ├── routes
│       └── services
├── data
│   ├── processed
│   │   └── graficos
│   └── raw
├── docs
├── etl
├── frontend
│   └── src
│       ├── components
│       │   ├── charts
│       │   └── dashboard
│       ├── pages
│       └── services
├── notebooks
├── scripts
│   └── mongo_atlas_loader

========================
ACLARACIÓN IMPORTANTE SOBRE SCRIPTS
===================================

La carpeta scripts NO debe eliminarse ni modificarse estructuralmente.

Actualmente ahí existen scripts para:

* creación de la base de datos local
* carga de datos
* automatización de MongoDB
* ejecución manual de procesos

Eso es intencional y correcto.

NO debes mover esa lógica.

La carpeta scripts debe mantenerse como:

* scripts administrativos
* scripts manuales
* loaders
* automatizaciones

NO debes mezclar scripts con lógica principal de negocio.

========================
CONTEXTO DEL PROYECTO
=====================

El proyecto es un sistema de análisis y visualización de estadísticas vitales de Colombia usando datos abiertos del DANE.

Los datasets incluyen:

* nacimientos
* defunciones fetales
* defunciones no fetales

Periodo:
2020–2024

Ya existen:

* ETL
* limpieza de datos
* transformación de datos
* datasets procesados
* scripts de carga MongoDB
* notebooks de análisis

MongoDB ya existe y contiene millones de registros.

El objetivo ahora es construir:

* backend FastAPI
* frontend Next.js
* dashboard interactivo
* autenticación
* visualización avanzada
* sistema de roles
* consultas analíticas

Usa @docs/proyecto.md como contexto principal del proyecto.

========================
STACK TECNOLÓGICO OBLIGATORIO
=============================

Frontend:

* Next.js 15
* React
* TypeScript
* TailwindCSS
* shadcn/ui
* Framer Motion
* Recharts o Apache ECharts
* Axios
* Zustand o Context API

Backend:

* FastAPI
* MongoDB
* Motor o PyMongo
* JWT Authentication
* Pydantic
* Python

========================
OBJETIVO DEL SISTEMA
====================

Quiero una plataforma moderna de analítica y Business Intelligence tipo:

* Power BI
* Tableau
* Datadog
* Stripe Dashboard
* Vercel Analytics

Debe verse:

* moderna
* premium
* profesional
* interactiva
* fluida
* lista para producción

NO quiero:

* dashboards genéricos
* interfaces saturadas
* exceso de gráficos inútiles

SÍ quiero:

* dashboards útiles
* análisis gerencial
* visualizaciones elegantes
* excelente UX/UI
* rendimiento
* escalabilidad

========================
ROLES DEL SISTEMA
=================

Implementa EXACTAMENTE estos roles:

1. Administrador
   Permisos:

* cargar datos
* crear usuarios
* ver usuarios
* administrar sistema
* acceso completo

2. Analista
   Permisos:

* crear filtros
* realizar consultas
* acceder a dashboards avanzados
* exportar resultados
* análisis avanzado

3. Usuario
   Permisos:

* solo visualizar dashboards
* acceso limitado de lectura

IMPORTANTE:

* El sidebar debe cambiar dinámicamente según el rol.
* Implementa control de acceso basado en roles.
* Protege rutas frontend y backend.
* Usa JWT Authentication.
* Implementa sesiones persistentes.

========================
BACKEND - REQUERIMIENTOS
========================

Debes construir un backend completo usando FastAPI.

La estructura backend/app debe mantenerse y completarse así:

backend/
└── app/
├── database/
├── models/
├── routes/
├── services/
├── schemas/
├── utils/
├── config/
└── main.py

========================
QUÉ DEBE IR EN CADA CARPETA
===========================

database/

* conexión MongoDB
* configuración Atlas/local
* cliente Mongo
* inicialización DB

models/

* modelos MongoDB
* User
* Natalidad
* Mortalidad
* Predicciones

routes/

* endpoints FastAPI
* auth routes
* dashboard routes
* analytics routes
* users routes
* upload routes

services/

* lógica de negocio
* agregaciones Mongo
* KPIs
* filtros
* consultas analíticas
* exportaciones
* predicciones

schemas/

* validaciones Pydantic
* requests
* responses
* DTOs

utils/

* JWT
* seguridad
* paginación
* helpers
* validadores

config/

* variables de entorno
* configuración global

main.py

* inicialización FastAPI
* middlewares
* CORS
* routers

========================
IMPORTANTE SOBRE CONSULTAS
==========================

MongoDB contiene millones de registros.

NO hagas:

* consultas gigantes al frontend
* envío masivo de datos
* cargas completas de colecciones

SÍ debes hacer:

* agregaciones MongoDB
* endpoints optimizados
* KPIs resumidos
* paginación
* filtros eficientes
* pipelines de agregación

Ejemplo correcto:

GET /dashboard/natalidad-by-year

Respuesta:
[
{ "year": 2020, "births": 500000 },
{ "year": 2021, "births": 520000 }
]

========================
ENDPOINTS IMPORTANTES
=====================

Debes crear endpoints como:

/auth/login
/auth/register
/auth/me

/dashboard/kpis
/dashboard/overview

/natalidad/by-year
/natalidad/by-department
/natalidad/by-gender

/mortalidad/by-year
/mortalidad/by-region

/analytics/comparison
/analytics/predictions

/users
/users/create
/users/list

/upload/dataset

========================
AUTENTICACIÓN
=============

Implementa:

* JWT Authentication
* login
* registro
* refresh token
* sesiones persistentes
* hashing de contraseñas
* middleware de autenticación
* protección por roles

========================
FRONTEND - REQUERIMIENTOS
=========================

Construye un frontend moderno usando Next.js 15.

Debes usar la estructura existente:

frontend/
└── src/
├── components/
├── pages/
└── services/

Puedes agregar:

* hooks
* layouts
* store
* utils

SIN reorganizar el proyecto completo.

========================
LANDING PAGE
============

Debe incluir:

* presentación del proyecto
* descripción del sistema
* KPIs destacados
* tecnologías usadas
* diseño SaaS moderno
* botón login
* animaciones modernas

========================
DASHBOARD PRINCIPAL
===================

Debe incluir:

* sidebar colapsable
* navbar superior
* filtros globales
* tarjetas KPI
* tabs
* skeleton loaders
* loading states
* tooltips
* animaciones
* responsive design

========================
MÓDULOS DEL DASHBOARD
=====================

1. Overview General

* KPIs
* resumen ejecutivo
* métricas generales

2. Natalidad

* nacimientos por año
* sexo
* edad
* departamentos
* educación
* régimen salud

3. Mortalidad

* defunciones por año
* causas
* distribución territorial
* grupos etarios

4. Análisis Territorial

* mapas
* rankings
* comparaciones regionales

5. Predicciones

* modelos ML ya entrenados
* tendencias futuras
* análisis predictivo

6. Administración
   SOLO ADMIN:

* usuarios
* carga datasets
* estado sistema

========================
DISEÑO VISUAL
=============

Quiero un diseño moderno tipo SaaS premium.

Inspiración:

* Stripe
* Linear
* Vercel
* Datadog
* Supabase
* Tremor

Usa:

* dark mode
* glassmorphism sutil
* gradientes
* sombras suaves
* animaciones fluidas
* excelente espaciado
* diseño limpio
* iconografía consistente

========================
GRÁFICOS
========

NO hagas gráficos sin sentido.

Implementa gráficos útiles como:

* líneas
* barras
* áreas
* heatmaps
* rankings
* comparaciones
* indicadores
* predicciones

NO satures la pantalla.

========================
INTEGRACIÓN FRONTEND-BACKEND
============================

MUY IMPORTANTE:
Explica e implementa claramente cómo se conecta el frontend con el backend.

Debes construir:

* servicios Axios
* interceptores
* manejo de tokens JWT
* hooks reutilizables
* manejo centralizado de errores
* tipado TypeScript
* consumo de APIs

Ejemplo:
frontend/src/services/api.ts

Debe incluir:

* baseURL
* interceptores
* auth headers
* refresh token handling

========================
MANEJO DE ESTADO
================

Usa:

* Zustand o Context API

Para:

* autenticación
* usuario actual
* filtros globales
* estado dashboard

========================
LO QUE DEBES GENERAR
====================

Genera:

1. estructura completa
2. backend completo
3. frontend completo
4. layouts
5. componentes reutilizables
6. autenticación
7. roles
8. rutas protegidas
9. dashboard moderno
10. gráficos
11. hooks
12. endpoints
13. integración frontend-backend
14. servicios API
15. ejemplos reales funcionales
16. buenas prácticas
17. optimización
18. arquitectura escalable

========================
IMPORTANTE
==========

Quiero código REAL y funcional.

NO solo mockups.
NO solo ejemplos superficiales.
NO pseudo código.

Quiero una implementación seria y profesional lista para continuar desarrollo real.

Prioriza:

* clean architecture
* SOLID
* modularidad
* escalabilidad
* mantenibilidad
* rendimiento
* UX/UI profesional
* buenas prácticas reales de producción
