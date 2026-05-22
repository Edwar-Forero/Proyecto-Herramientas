from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import ROLE_LABELS, ROLES, USUARIOS
from app.schemas.auth import LoginRequest, RegisterRequest, TokenResponse, UserResponse
from app.utils.jwt_handler import create_access_token, create_refresh_token, safe_decode, verify_token_type
from app.utils.security import hash_password, verify_password


class AuthService:
    async def login(self, db: AsyncIOMotorDatabase, data: LoginRequest) -> TokenResponse:
        user = await db[USUARIOS].find_one({"username": data.username, "activo": True})
        if not user or not verify_password(data.password, user.get("password_hash", "")):
            raise ValueError("Credenciales inválidas")

        access = create_access_token(user["username"], user["rol"])
        refresh = create_refresh_token(user["username"], user["rol"])
        return TokenResponse(access_token=access, refresh_token=refresh)

    async def register(self, db: AsyncIOMotorDatabase, data: RegisterRequest) -> UserResponse:
        if data.rol not in ROLES:
            raise ValueError("Rol inválido")
        existing = await db[USUARIOS].find_one({"username": data.username})
        if existing:
            raise ValueError("El usuario ya existe")

        doc = {
            "username": data.username,
            "password_hash": hash_password(data.password),
            "rol": data.rol,
            "activo": True,
        }
        result = await db[USUARIOS].insert_one(doc)
        return UserResponse(
            id=str(result.inserted_id),
            username=data.username,
            rol=data.rol,
            rol_label=ROLE_LABELS.get(data.rol, data.rol),
            activo=True,
        )

    async def refresh(self, db: AsyncIOMotorDatabase, refresh_token: str) -> TokenResponse:
        payload = safe_decode(refresh_token)
        if not payload or not verify_token_type(payload, "refresh"):
            raise ValueError("Refresh token inválido")

        username = payload.get("sub")
        user = await db[USUARIOS].find_one({"username": username, "activo": True})
        if not user:
            raise ValueError("Usuario no encontrado")

        return TokenResponse(
            access_token=create_access_token(user["username"], user["rol"]),
            refresh_token=create_refresh_token(user["username"], user["rol"]),
        )

    async def me(self, db: AsyncIOMotorDatabase, username: str) -> UserResponse:
        user = await db[USUARIOS].find_one({"username": username, "activo": True})
        if not user:
            raise ValueError("Usuario no encontrado")
        return UserResponse(
            id=str(user["_id"]),
            username=user["username"],
            rol=user["rol"],
            rol_label=ROLE_LABELS.get(user["rol"], user["rol"]),
            activo=user.get("activo", True),
        )


auth_service = AuthService()
