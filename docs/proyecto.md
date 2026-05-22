# Universidad del Valle
## Sede Tuluá
**Fecha:** 09-02-2026  
**Facultad de Ingeniería** **Programa:** Ingeniería de Sistemas  
**Asignatura:** 750030C - Herramientas para la Gestión de Proyectos de Tecnologías de la Información  
**Docente:** Doc. M.Sc. Adrian Lasso C - luis.lasso@correounivalle.edu.co  

---

## Enunciado Proyecto Final (Valor: 60%)

### Conceptos iniciales
El análisis de datos es una de las tareas fundamentales del Business Intelligence (BI) o Inteligencia Empresarial, que permite a las organizaciones identificar patrones de comportamiento, tendencias y demás factores relevantes que permiten mejorar el proceso de toma de decisiones del sector analizado.

En este sentido, el BI se puede definir como un software que se alimenta de datos del negocio, los procesa y presenta en vistas fáciles llamados **dashboard** (tablero o cuadro de mando), los cuales están compuestos por paneles, tablas y gráficos que resumen el estado actual de la organización.

**Enunciado:** Construir una aplicación de BI tipo dashboard, para realizar un análisis de datos de tipo gerencial, con enfoque al apoyo de toma de decisiones, de cualquier entidad (alcaldía, gobernación, instituciones del estado, ..., etc.) que implemente en su página web "Datos abiertos".

---

### Requerimientos

1. **Los temas a analizar son:**
   * Agricultura
   * Educación
   * Medio Ambiente
   * Movilidad
   * Postconflicto
   * Salud
   * Seguridad
   * Turismo
   * Tecnología
   * Trabajo
   * Otro (socializado con el docente)

2. Los datos a analizar deben ser de **por lo menos cinco años**. Por ejemplo; datos de graduados de IES de Colombia de los años 2021, 2022, 2023, 2024 y 2025. Los años no necesariamente deben ser consecutivos.

3. Tener en cuenta que, en algunas ocasiones, los datos suministrados de cada año no son homogéneos, es decir no siempre tienen los mismos campos (columnas), por lo cual se debe realizar una depuración de los mismos. Esto hace parte del **proceso ETL**.

4. Después de aplicar el proceso de transformación, los datos resultantes deben contener **por lo menos 5.000.000 de filas (registros) en total** (sumando todos los esquemas de la BD).

5. Se debe usar una **metodología enfocada en gestionar proyectos de ciencia de datos**, análisis y minería de datos, dentro de las cuales se pueden mencionar: CRISP-DM (*Cross-Industry Standard Process for Data Mining*), SEMMA (*Sample, Explore, Modify, Model, Assess*), KDD (*Knowledge Discovery in Databases*), entre otros.

6. Para el almacenamiento de datos se debe usar un **SGBD NoSQL** (orientado al Big Data). Por ejemplo; Cassandra, MongoDB, Apache HBase, Amazon Redshift, entre otros.

7. El código fuente debe cumplir **buenas prácticas de programación**.

8. La aplicación debe construirse utilizando **al menos tres patrones de diseño** (MVC, Builder, Singleton, ....).

9. El sistema tiene **por lo menos 3 tipos de usuario**, y cada uno tiene acceso a diferentes funcionalidades del sistema. El rol de cada usuario es definido por el equipo de trabajo.

10. Todos los usuarios deben **loguearse** para utilizar la aplicación.

11. La aplicación muestra un **menú con opciones** dependiendo del tipo de usuario logueado.

12. Se deben implementar o usar **librerías de IA, Data Mining o Machine Learning** existentes para el análisis de datos, que permitan describir el estado actual del negocio y predecir futuros escenarios.

13. La aplicación puede ser desarrollada con ayuda de **cualquier framework, paquete o similares**, propios del lenguaje de programación seleccionado.

14. **Diez requerimientos funcionales adicionales** especificados por el equipo de trabajo.

---

### Algunas plataformas que implementan Datos Abiertos:
* https://www.datos.gov.co/
* https://microdatos.dane.gov.co/index.php/catalog/central/about
* https://www.mineducacion.gov.co/portal/micrositios-institucionales/Modelo-Integrado-de-Planeacion-y-Gestion/Datos-abiertos/349303:Datos-Abiertos
* https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/
* https://www.datos.gov.co/Educaci-n/Resultados-nicos-Saber-Pro/u37r-hjmu/about data
* https://geoportal.igac.gov.co/contenido/datos-abiertos-igac
* https://www.valledelcauca.gov.co/datos abiertos/

---

### Ejemplos de GUI - Dashboard
*(El documento original incluye imágenes de referencia de tableros de control como "Panel de Análisis Económico" y "Dashboard - Análisis de la Deserción Laboral" con métricas, gráficos de barras, líneas y torta).*

---

### Observaciones generales

* **Metodología de trabajo:** grupos de máximo cinco estudiantes.
* **Pitch - Socialización de la propuesta:** semana 4 (02-03-2026).
* En el campus se encuentran disponibles las plantillas de Word para el documento y para la exposición final.
* **Método de entrega:** campus virtual.

### Entregas:

1. **Semana 6. 16-03-2026 14:00 (10%):** Portada + contraportada + Tabla de contenido + Capítulos 1 al 6 de la plantilla de Word + Bibliografía utilizada.
2. **Semana 11. 27-04-2026 14:00 (20%):** Corrección entrega 1 + Capítulos 7 y 8 de la plantilla de Word + prototipo de la aplicación que implementa la mitad de las historias de usuario.
3. **Semana 16. 01-06-2026 14:00 (30%):** Corrección entrega 2 + Documento completo plantilla de Word + Manual de usuario + Aplicación que cumple con la totalidad de los requerimientos y la implementación de todas las historias de usuario + Diapositivas (PDF) + URL vídeo de sustentación.

---

### Criterios adicionales para la entrega final:

1. **No escribir nada en la caja de comentarios de la entrega.** Estos no serán leídos, ni tenidos en cuenta para la evaluación. Toda la información relacionada debe aparecer en los documentos a entregar.
2. **Video que muestre:**
   * La presentación del proyecto mediante diapositivas (plantilla disponible en el campus).
   * Evidenciar todas las estrategias y herramientas usadas para la gestión del proyecto.
   * Mostrar la funcionalidad total de la aplicación.
   * En el video deben intervenir **todos los integrantes del grupo de trabajo** tanto en la presentación (diapositivas), como en la explicación de la aplicación (código fuente y funcionalidad).
   * Al momento de la sustentación todos los integrantes del grupo de trabajo deben **activar la cámara de video**.
   * Duración máxima de **15 min**.
   * Publicado en **YouTube** o compartido en **Google Drive**.
   * Dentro de la carpeta del proyecto se anexa un archivo de texto con la URL del vídeo de YouTube o Google Drive.
   * **No se admiten entregas de la URL como comentario en el campus.** El campus no reproduce el video (queda una ventana negra).

> ⚠️ **Nota importante:** Antes de realizar la entrega, verificar la correcta reproducción del video en YouTube (permisos de acceso, calidad de imagen, audio, presentación, etc.). Asegurarse de que el video lo pueda reproducir cualquier usuario desde cualquier cuenta de correo (acceso libre/público). 
> 
> En general, **no se admiten entregas por fuera de plazo** ni reclamos posteriores.