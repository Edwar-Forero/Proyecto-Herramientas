from fastapi import APIRouter, Depends, HTTPException, status
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.schemas.user import UserCreate, UserListResponse, UserOut
from app.services.user_service import user_service
from app.utils.dependencies import AdminUser

router = APIRouter(prefix="/users", tags=["Usuarios"])


@router.post("/create", response_model=UserOut, status_code=status.HTTP_201_CREATED)
async def create_user(
    data: UserCreate,
    current_user: AdminUser,
    db: AsyncIOMotorDatabase = Depends(get_db),
):
    try:
        return await user_service.create(db, data)
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc


@router.get("/list", response_model=UserListResponse)
async def list_users(current_user: AdminUser, db: AsyncIOMotorDatabase = Depends(get_db)):
    return await user_service.list_users(db)
