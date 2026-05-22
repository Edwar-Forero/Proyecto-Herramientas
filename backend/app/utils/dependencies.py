"""Dependencias FastAPI — autenticación y control de roles (Strategy)."""

from typing import Annotated, Callable

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.database import get_db
from app.models import ROLE_LABELS, ROLES
from app.utils.jwt_handler import safe_decode, verify_token_type

security = HTTPBearer(auto_error=False)


class RoleChecker:
    """Patrón Strategy para validar permisos por rol."""

    def __init__(self, allowed_roles: tuple[str, ...]) -> None:
        self.allowed_roles = allowed_roles

    async def __call__(
        self,
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
        db: Annotated[AsyncIOMotorDatabase, Depends(get_db)],
    ) -> dict:
        if credentials is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "No autenticado")

        payload = safe_decode(credentials.credentials)
        if not payload or not verify_token_type(payload, "access"):
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token inválido o expirado")

        username = payload.get("sub")
        if not username:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token inválido")

        user = await db["usuarios"].find_one({"username": username, "activo": True})
        if not user:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Usuario no encontrado")

        rol = user.get("rol", "consulta")
        if rol not in self.allowed_roles:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Permisos insuficientes")

        return {
            "id": str(user["_id"]),
            "username": user["username"],
            "rol": rol,
            "rol_label": ROLE_LABELS.get(rol, rol),
        }


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    db: Annotated[AsyncIOMotorDatabase, Depends(get_db)],
) -> dict:
    return await RoleChecker(ROLES)(credentials, db)


require_admin = RoleChecker(("admin",))
require_analista = RoleChecker(("admin", "analista"))
require_any_auth = RoleChecker(ROLES)

CurrentUser = Annotated[dict, Depends(get_current_user)]
AdminUser = Annotated[dict, Depends(require_admin)]
AnalistaUser = Annotated[dict, Depends(require_analista)]
