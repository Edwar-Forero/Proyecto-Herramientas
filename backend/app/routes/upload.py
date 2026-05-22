from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.services.upload_service import upload_service
from app.utils.dependencies import AdminUser

router = APIRouter(prefix="/upload", tags=["Carga de datos"])


@router.post("/dataset")
async def upload_dataset(
    current_user: AdminUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
    file: UploadFile = File(...),
    collection: str = Form(...),
    replace: bool = Form(False),
):
    if not file.filename or not file.filename.endswith(".parquet"):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Solo se aceptan archivos .parquet")

    content = await file.read()
    try:
        return await upload_service.upload_parquet(db, content, collection, replace)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc)) from exc
