# 📊 Sistema de Análisis y Visualización de Estadísticas Vitales en Colombia (2020–2024)

> **Proyecto académico** — Herramientas para la Gestión de Proyectos de TI  
> Universidad del Valle · Ingeniería de Sistemas · 9° semestre · 2026

---

## 👥 Equipo

| Nombre | Código | Correo |
|--------|--------|--------|
| Edwar Yamir Forero Blanco | 2559741 | edwar.forero@correounivalle.edu.co |
| Juan Alejandro Urrego | 2569068 | juan.alejandro.urrego@correounivalle.edu.co |
| Jhojan Serna Henao | 2259504 | jhojan.serna@correounivalle.edu.co |
| Kevin Hinojosa Osorio | 2259470 | kevin.hinojosa@correounivalle.edu.co |
| Jaider Bermudez Girón | 2569453 | jaider.bermudez@correounivalle.edu.co |

---

## 🚀 Arranque rápido

```powershell
# Primera vez — instala dependencias Python + Node
powershell -ExecutionPolicy Bypass -File .\dev.ps1 -Setup

# Desarrollo diario (un solo comando)
npm run dev
```

| Servicio | URL |
|----------|-----|
| Frontend (Next.js) | http://localhost:3000 |
| Backend (FastAPI) | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/docs |

---

## 🏗️ Arquitectura Three-Tier

```
┌─────────────────────────────────────────────────────┐
│  CAPA DE PRESENTACIÓN · Next.js 15 + React 19       │
│  TypeScript · Recharts · Tailwind · Framer Motion   │
└──────────────────────┬──────────────────────────────┘
                       │  REST / JSON  (HTTP)
┌──────────────────────▼──────────────────────────────┐
│  CAPA DE NEGOCIO · FastAPI + Python 3.12            │
│  JWT Auth · Regresión Lineal (scikit-learn)         │
│  Motor (async MongoDB driver) · Pydantic v2         │
└──────────────────────┬──────────────────────────────┘
                       │  Motor async
┌──────────────────────▼──────────────────────────────┐
│  CAPA DE DATOS · MongoDB Atlas                      │
│  3 colecciones: nacimientos · def. fetales          │
│                 def. no fetales                     │
│  ~5 años de datos DANE 2020–2024                    │
└─────────────────────────────────────────────────────┘
```

### Tecnologías clave

| Capa | Tecnología | Rol |
|------|-----------|-----|
| Frontend | Next.js 15, TypeScript | SPA + SSR, enrutamiento |
| UI | Tailwind CSS, Radix UI | Componentes accesibles |
| Gráficas | Recharts 2 | Visualizaciones interactivas |
| Backend | FastAPI, Python 3.12 | API REST asíncrona |
| ML | scikit-learn | Regresión lineal de predicción |
| BD | MongoDB Atlas | Almacenamiento de registros vitales |
| Auth | JWT + bcrypt | Autenticación stateless |
| ETL | Pandas, PyArrow | Carga y transformación de datos DANE |

---

## 👤 Tipos de Usuario

| Rol | Acceso | Capacidades |
|-----|--------|------------|
| **Visitante** | `/` (landing) | Solo lee información general del sistema |
| **Usuario** | Dashboard básico | Ve KPIs, natalidad y mortalidad por año y departamento |
| **Analista** | Dashboard completo | Accede a análisis comparativo, exportación CSV y predicciones |
| **Administrador** | Panel de admin | Gestión de usuarios, carga de datasets, estado del sistema |

---

## 📈 Módulos y Gráficas del Sistema

### 🏠 Dashboard Principal
Vista general con indicadores clave del período seleccionado.

| Gráfica / KPI | Descripción |
|---------------|-------------|
| **KPIs (5 tarjetas)** | Totales de nacimientos, defunciones fetales, no fetales, eventos totales y tasa fetal/nacimientos |
| **Nacimientos por año** | Área chart con la tendencia anual de nacimientos 2020–2024 |
| **Defunciones totales por año** | Suma de fetales + no fetales por año |
| **Top departamentos** | Ranking de los 8 departamentos con más nacimientos |

---

### 🍼 Natalidad
Análisis detallado de los nacimientos registrados en Colombia.

| Gráfica | Tipo | Descripción |
|---------|------|-------------|
| **Nacimientos por año** | Área | Tendencia temporal de registros de nacimientos |
| **Distribución por sexo** | Pie | Proporción de nacimientos masculinos vs femeninos |
| **Top departamentos** | Barra horizontal | Los departamentos con mayor número de nacimientos |
| **Nivel educativo materno** | Barra horizontal | Escolaridad de la madre al momento del parto |
| **Régimen de seguridad social** ⭐ | Pie | Tipo de afiliación al sistema de salud (contributivo, subsidiado, etc.) |
| **Nacimientos por edad de la madre** ⭐ | Barra horizontal | Distribución por grupos de edad materna |

---

### 💀 Mortalidad
Análisis de defunciones fetales y no fetales.

| Gráfica | Tipo | Descripción |
|---------|------|-------------|
| **Composición de defunciones** ⭐ | Área apilada | Visualización acumulada fetal vs no fetal por año |
| **Defunciones fetales por año** | Área | Tendencia anual de defunciones fetales |
| **Defunciones no fetales por año** | Área | Tendencia anual de defunciones no fetales |
| **Situación de la defunción fetal** | Barra horizontal | Clasificación por circunstancia (parto, embarazo, etc.) |
| **Grupos etarios no fetales** | Barra horizontal | Distribución de defunciones por rango de edad |

---

### 🗺️ Análisis Territorial
Comparación geográfica entre departamentos de Colombia.

| Gráfica | Tipo | Descripción |
|---------|------|-------------|
| **Nacimientos por departamento** | Barra horizontal | Ranking completo de los 10 principales departamentos |
| **Defunciones fetales por depto.** | Barra horizontal | Ranking departamental de mortalidad fetal |
| **Defunciones no fetales por depto.** | Barra horizontal | Ranking departamental de mortalidad no fetal |

---

### 📊 Análisis Comparativo *(solo Analista / Admin)*
Comparación entre los tres tipos de eventos vitales.

| Gráfica / Acción | Descripción |
|-----------------|-------------|
| **Comparación anual agrupada** | BarChart con 3 series (natalidad, fetal, no fetal) por año |
| **Exportar CSV** | Descarga los datos de comparación en formato CSV |

---

### 🤖 Predicciones *(solo Analista / Admin)*
Proyecciones estadísticas a 3 años usando regresión lineal.

| Elemento | Descripción |
|----------|-------------|
| **Gráfica Nacimientos** ⭐ | Serie histórica (área sólida) + proyección 3 años (línea punteada) con badge R² |
| **Gráfica Mortalidad Fetal** ⭐ | Mismo formato combinado histórico + proyección |
| **Gráfica Mortalidad No Fetal** ⭐ | Mismo formato combinado histórico + proyección |
| **Tabla de valores proyectados** | Tabla con valores numéricos para cada año proyectado |
| **Indicadores R²** | Coeficiente de determinación del modelo (0–1) con interpretación de calidad |

> **¿Qué es R²?** Mide qué tan bien la línea de regresión describe la tendencia histórica.  
> R² > 0.85 = buen ajuste · R² > 0.60 = moderado · R² ≤ 0.60 = ajuste bajo.

---

### ⚙️ Panel de Administración *(solo Admin)*

| Función | Descripción |
|---------|-------------|
| **Gestión de usuarios** | Crear usuarios con roles: `admin`, `analista`, `usuario` |
| **Carga de datasets** | Subir archivos Parquet o CSV para actualizar las colecciones MongoDB |
| **Estado del sistema** | Ver cantidad de registros por colección y estado de la base de datos |

---

## 🔒 Seguridad

- Autenticación con **JWT** (access token 30 min + refresh token 7 días)
- Rutas protegidas por **rol** (RBAC): cada endpoint valida el token y el rol
- **CORS** configurado para aceptar solo orígenes permitidos

---

## 📁 Estructura del proyecto

```
Proyecto-Herramientas/
├── backend/              # FastAPI + Python
│   ├── app/
│   │   ├── routes/       # Endpoints REST
│   │   ├── services/     # Lógica de negocio + ML
│   │   ├── schemas/      # Pydantic models
│   │   └── database/     # Conexión MongoDB (Motor)
│   └── requirements.txt
├── frontend/             # Next.js + TypeScript
│   └── src/
│       ├── pages/        # Páginas del dashboard
│       ├── components/   # Gráficas y UI
│       └── services/     # Llamadas a la API
├── etl/                  # Scripts de carga de datos
├── notebooks/            # Análisis exploratorio (Jupyter)
├── data/                 # Datasets originales DANE
├── dev.ps1               # Script unificado (Windows)
├── dev.sh                # Script unificado (Linux/Mac)
└── package.json          # npm run dev → levanta todo
```

---

## 📦 Pipeline de Datos (ETL)

```
Datasets DANE (CSV/Parquet)
        │
        ▼  Jupyter Notebooks
  Limpieza + transformación (Pandas)
        │
        ▼  Scripts ETL Python
  Carga a MongoDB Atlas
        │
        ▼  FastAPI (Motor async)
  API REST → Frontend Next.js
```

---

*Datos fuente: DANE — Estadísticas Vitales de Colombia 2020–2024*