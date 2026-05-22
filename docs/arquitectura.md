# Arquitectura del Proyecto — Estadísticas Vitales Colombia (BI Dashboard)

Este documento explica en detalle cómo está estructurado el proyecto, cómo se conectan el Frontend y el Backend, cómo interactúan con la base de datos (MongoDB) y dónde están implementadas las funciones analíticas clave (como los modelos de predicción con Scikit-Learn).

## Índice
1. [Visión General (Conexión Frontend ↔ Backend ↔ Base de Datos)](#1-visión-general)
2. [Estructura del Backend (FastAPI)](#2-estructura-del-backend)
3. [Estructura del Frontend (Next.js)](#3-estructura-del-frontend)
4. [Implementación del Modelo de Predicción (LinearRegression)](#4-modelo-de-predicción)
5. [Guía Rápida para Hacer Cambios](#5-guía-rápida-para-hacer-cambios)

---

## 1. Visión General

El proyecto sigue una arquitectura **Cliente-Servidor (API REST)** moderna:

* **Frontend (Cliente)**: Aplicación React (Next.js 15) que renderiza la interfaz, gráficas y gestiona la sesión (Zustand). Realiza peticiones HTTP (mediante Axios) al Backend.
* **Backend (Servidor)**: Aplicación Python (FastAPI). Expone endpoints (Rutas) que el frontend consume. Procesa lógica de negocio, autenticación, y delegación de cálculos matemáticos.
* **Base de Datos**: MongoDB Atlas (NoSQL). El backend se conecta de forma asíncrona usando la librería `Motor`. Toda la data de DANE se almacena en colecciones documentales masivas.

### ¿Cómo se comunican?
1. El usuario interactúa con un filtro en la interfaz (ej. cambiar el año).
2. El store de `Zustand` (en Frontend) detecta el cambio e invoca a los servicios de `api.ts` (Axios).
3. Axios hace una solicitud HTTP `GET /api/dashboard/overview?year_from=2020...` al backend, inyectando el token JWT (si hay un usuario logueado).
4. FastAPI (Backend) intercepta la petición, verifica los permisos mediante el archivo `dependencies.py` (Autenticación) y le pasa los parámetros a los servicios (`services/`).
5. El servicio de Backend arma una *Aggregation Pipeline* de MongoDB y hace una única consulta eficiente a la base de datos para extraer los totales de los millones de registros.
6. MongoDB responde con el resumen numérico.
7. FastAPI retorna esto en formato JSON al Frontend.
8. Los componentes gráficos (`Recharts`) se actualizan automáticamente y re-dibujan la gráfica.

---

## 2. Estructura del Backend

Se encuentra en la carpeta `backend/`. El corazón es la carpeta `app/`.

### Carpetas Principales de `backend/app/`

* `config/`: 
  * Contiene `settings.py`. Este archivo lee las variables de entorno de `.env` (credenciales, secretos JWT, URL de Mongo). Es un Singleton: se carga en memoria 1 sola vez.
* `database/`: 
  * Contiene `mongodb.py`. Maneja la conexión asíncrona usando el motor `AsyncIOMotorClient`. Maneja el ciclo de vida (conectarse al iniciar, desconectarse al apagar).
* `models/`: 
  * `collections.py` y `schemas.py`. Definen los nombres en duro de las colecciones de base de datos (ej. "nacimientos", "usuarios") y llaves importantes. Ayuda a evitar errores de tipeo.
* `schemas/`: 
  * Modelos de *Pydantic* (ej. `auth.py`, `dashboard.py`). Validan que los datos que entran y salen de la API tengan el formato correcto y obligan a que siempre se devuelva lo que el Frontend espera.
* `utils/`: 
  * `dependencies.py`: Lógica de inyección. Acá se validan los Roles (Admin, Analista, Consulta).
  * `jwt_handler.py` y `security.py`: Lógica de encriptación de contraseñas y creación/verificación de los Tokens JWT.
* `routes/`: (Los Controladores)
  * Estos archivos (ej. `auth.py`, `analytics.py`, `dashboard.py`) **solo declaran URLs** (los endpoints). Reciben peticiones y delegan el trabajo duro a la carpeta `services/`.
* `services/`: (Lógica de Negocio Pura)
  * **Aquí vive el código pesado**.
  * `aggregation_factory.py`: Archivo vital. Como la DB tiene 5+ millones de registros, no podemos traerlos a memoria. Aquí se construyen "Pipelines de agregación" para que MongoDB cuente y sume datos internamente de manera ultra-rápida.
  * `analytics_service.py` y `prediction_service.py`: Ejecutan la ciencia de datos.

---

## 3. Estructura del Frontend

Se encuentra en la carpeta `frontend/`. Utiliza Next.js en su patrón "Pages Router" (`src/pages/`).

### Carpetas Principales de `frontend/src/`

* `pages/`: 
  * Mapea directamente a rutas URL. Por ejemplo, `pages/login.tsx` es tu ruta `/login`. La carpeta `pages/dashboard/` contiene todas las sub-vistas del panel.
* `components/`: 
  * `ui/`: Componentes genéricos, tontos, reusables, estilizados con Tailwind (Botones, Inputs, Cards).
  * `dashboard/`: Componentes específicos de la app (Sidebar, Navbar, GlobalFilters).
  * `charts/`: Componentes empaquetadores de `Recharts` (Barras, Líneas, etc.) listos para recibir datos.
* `layouts/`: 
  * `DashboardLayout.tsx`: Contiene la estructura global. Protege la página para que usuarios sin iniciar sesión sean redirigidos, y dibuja la Sidebar y Navbar antes de inyectar el contenido central.
* `services/`: 
  * `api.ts`: Configura `Axios`. Intercepta toda petición saliente y le pega tu Token de autenticación de forma invisible. Maneja también el "Refresh Token" de forma automática si la sesión caduca.
  * `dashboard.ts`, `auth.ts`: Funciones puente. En lugar de escribir código HTTP feo en los componentes, solo llamas por ejemplo a `getOverview()`.
* `store/`: 
  * `authStore.ts` y `filterStore.ts`: Utiliza **Zustand**. Variables globales que todos los componentes pueden leer. Si cambias el filtro de año en la Navbar (que está arriba), el store global cambia y fuerza a las gráficas (que están en el centro) a re-cargarse.
* `types/`: 
  * `api.ts`: Archivo de TypeScript donde se declaran las interfaces (formas de los objetos de datos) que devuelve el Backend para que el editor de código pueda auto-completar.

---

## 4. Modelo de Predicción (Scikit-Learn)

### ¿Dónde está implementado?
El modelo de *Machine Learning* predictivo de Regresión Lineal está alojado en el Backend, específicamente en:
📍 **`backend/app/services/prediction_service.py`**

### ¿Cómo funciona?
1. En el archivo `prediction_service.py`, existe una clase `PredictionService`.
2. Esta clase hace una consulta rápida a la base de datos agrupada por año (desde 2020 hasta el presente) utilizando `aggregation_factory.py`.
3. Trae series temporales básicas (Ejemplo: `[(2020: 600k), (2021: 580k), (2022: 560k)]`).
4. Importa `from sklearn.linear_model import LinearRegression`.
5. Prepara el eje X (Años) y el eje Y (Valor a predecir, ej. Nacimientos).
6. Entrena el modelo dinámicamente "al vuelo" con `model.fit(X, Y)`.
7. Hace proyecciones futuras (ej. 2025, 2026, 2027) con `model.predict(next_years)`.
8. Retorna todo junto en formato JSON para que el Frontend lo grafique.

*El frontend simplemente llama al endpoint de `/analytics/predictions` a través del servicio de React y grafica lo devuelto.*

---

## 5. Guía Rápida para Hacer Cambios

### Escenario A: Quiero agregar un filtro nuevo (ej. Buscar por Ciudad)
1. **Frontend (Filtro Visual)**: Agrégalo en `frontend/src/components/dashboard/GlobalFilters.tsx`.
2. **Frontend (Estado)**: Modifica `frontend/src/store/filterStore.ts` para que guarde el estado `ciudad`.
3. **Backend (Consultas DB)**: Ve a `backend/app/services/aggregation_factory.py` y agrega la ciudad a `match_filters()`. *¡Casi mágico, todas tus gráficas empezarán a obedecer el filtro de ciudad automáticamente!*

### Escenario B: Quiero cambiar el modelo matemático
1. Abre `backend/app/services/prediction_service.py`.
2. Importa el nuevo modelo que desees, por ejemplo, `from sklearn.ensemble import RandomForestRegressor`.
3. Cambia la línea `model = LinearRegression()` a tu nuevo modelo. Reinicia el backend (`start_backend.sh`) y listo.

### Escenario C: Quiero añadir una nueva Gráfica al Dashboard Principal
1. Crea un Endpoint si no lo tienes en el backend (`backend/app/routes/dashboard.py`).
2. Crea el tipado (TypeScript) en `frontend/src/types/api.ts` de la respuesta que darás.
3. Agrégalo en `frontend/src/pages/dashboard/index.tsx`. Utiliza `BarChartCard` o `LineChartCard` (de `components/charts/`) que están preparados para consumir listas de datos directamente.
