# Plan de Implementación — Pasos Faltantes del Proyecto BI

## Estado actual del proyecto

El proyecto tiene una **base sólida y casi completa**. La arquitectura definida está respetada. Lo que falta son:
- Archivos stub vacíos (0 bytes) que necesitan contenido real
- Un bug crítico en `admin.tsx` que impide compilar
- Scripts de arranque para correr backend y frontend
- Pequeñas mejoras de pulido (CSS animations, globals)

---

## Problemas Encontrados

> [!CAUTION]
> **Bug crítico en `admin.tsx` línea 137**: `</motion>` es un tag inválido que rompe la compilación del frontend. Debe ser `</div>`.

> [!WARNING]
> **6 archivos backend vacíos (0 bytes)**: `models/schemas.py`, `services/analytics.py`, `services/queries.py`, `routes/fetales.py`, `routes/nacimientos.py`, `routes/no_fetales.py` — aunque no rompen el servidor (no están importados), deben completarse.

> [!WARNING]
> **5 archivos frontend vacíos**: `pages/Fetales.jsx`, `pages/Nacimientos.jsx`, `pages/NoFetales.jsx`, `services/api.js`, `components/dashboard/prueba.jsx`, `components/charts/prueba.jsx` — causan errores de lint y confusión.

---

## Proposed Changes

### 1. Bug Fix Crítico — Frontend

#### [MODIFY] [admin.tsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/pages/dashboard/admin.tsx)
- Corregir línea 137: `</motion>` → `</div>`

---

### 2. Archivos Backend Vacíos — Completar con contenido real

#### [MODIFY] [models/schemas.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/models/schemas.py)
- Agregar constantes de campos MongoDB por colección (campos usados en aggregations)

#### [MODIFY] [services/analytics.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/services/analytics.py)
- Re-exportar `analytics_service` para compatibilidad + agregar función `top_causes_nofetal`

#### [MODIFY] [services/queries.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/services/queries.py)
- Helpers predefinidos de consultas MongoDB frecuentes (shortcuts de aggregation)

#### [MODIFY] [routes/nacimientos.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/routes/nacimientos.py)
- Endpoint específico `/nacimientos/summary` — resumen rápido de nacimientos

#### [MODIFY] [routes/fetales.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/routes/fetales.py)
- Endpoint `/fetales/summary` — resumen de defunciones fetales

#### [MODIFY] [routes/no_fetales.py](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/backend/app/routes/no_fetales.py)
- Endpoint `/no-fetales/summary` — resumen de defunciones no fetales

---

### 3. Archivos Frontend Vacíos — Completar

#### [MODIFY] [pages/Nacimientos.jsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/pages/Nacimientos.jsx)
- Redirect a `/dashboard/natalidad`

#### [MODIFY] [pages/Fetales.jsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/pages/Fetales.jsx)
- Redirect a `/dashboard/mortalidad`

#### [MODIFY] [pages/NoFetales.jsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/pages/NoFetales.jsx)
- Redirect a `/dashboard/mortalidad`

#### [MODIFY] [services/api.js](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/services/api.js)
- Re-export de `api.ts` para compatibilidad JS

#### [MODIFY] [components/dashboard/prueba.jsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/components/dashboard/prueba.jsx) / [components/charts/prueba.jsx](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/components/charts/prueba.jsx)
- Agregar componentes de prueba/testing simples

---

### 4. Scripts de Arranque — [NEW]

#### [NEW] start_backend.ps1
- Script PowerShell para instalar deps y arrancar uvicorn

#### [NEW] start_frontend.ps1
- Script PowerShell para instalar deps y arrancar Next.js dev server

#### [NEW] setup_admin.py
- Script para crear usuario admin inicial en MongoDB (en `scripts/`)

---

### 5. Mejoras de CSS — globals.css

#### [MODIFY] [globals.css](file:///c:/Users/edwar/Univalle_9no/HERRAMIENTAS%20PARA%20LA%20GESTIÓN%20DE%20PROYECTOS/Proyecto-Herramientas/frontend/src/styles/globals.css)
- Agregar utilidades faltantes: `animate-fade-in`, scrollbar styling, glassmorphism adicional

---

## Verification Plan

### Backend
- Correr `python -m uvicorn app.main:app --reload` desde `backend/`
- Verificar `/health` responde `{"status":"ok"}`
- Verificar `/docs` muestra todos los endpoints

### Frontend
- Correr `npm run dev` desde `frontend/`
- Verificar que compila sin errores TypeScript
- Verificar login funciona, sidebar carga según rol
