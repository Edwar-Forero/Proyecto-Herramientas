from fastapi import APIRouter

router = APIRouter(prefix="/fetales-legacy", tags=["Mortalidad Fetal Legacy"])

# Rutas delegadas a mortalidad.py en la nueva arquitectura
@router.get("/summary")
async def summary():
    return {"message": "Use /mortalidad/by-year instead. This is for backwards compatibility."}
