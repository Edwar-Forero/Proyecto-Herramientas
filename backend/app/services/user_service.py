from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import ROLE_LABELS, ROLES, USUARIOS
from app.schemas.user import UserCreate, UserListResponse, UserOut
from app.utils.security import hash_password


class UserService:
    async def create(self, db: AsyncIOMotorDatabase, data: UserCreate) -> UserOut:
        if data.rol not in ROLES:
            raise ValueError("Rol inválido")
        if await db[USUARIOS].find_one({"username": data.username}):
            raise ValueError("El usuario ya existe")

        doc = {
            "username": data.username,
            "password_hash": hash_password(data.password),
            "rol": data.rol,
            "activo": True,
        }
        result = await db[USUARIOS].insert_one(doc)
        return UserOut(
            id=str(result.inserted_id),
            username=data.username,
            rol=data.rol,
            rol_label=ROLE_LABELS.get(data.rol, data.rol),
            activo=True,
        )

    async def list_users(self, db: AsyncIOMotorDatabase) -> UserListResponse:
        cursor = db[USUARIOS].find({}, {"password_hash": 0}).sort("username", 1)
        users = []
        async for doc in cursor:
            rol = doc.get("rol", "consulta")
            users.append(
                UserOut(
                    id=str(doc["_id"]),
                    username=doc["username"],
                    rol=rol,
                    rol_label=ROLE_LABELS.get(rol, rol),
                    activo=doc.get("activo", True),
                )
            )
        return UserListResponse(users=users, total=len(users))


user_service = UserService()
