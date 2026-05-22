from fastapi import APIRouter

router = APIRouter(prefix="/nacimientos-legacy", tags=["Nacimientos Legacy"])

# Rutas delegadas a natalidad.py en la nueva arquitectura
@router.get("/summary")
async def summary():
    return {"message": "Use /natalidad/by-year instead. This is for backwards compatibility."}
