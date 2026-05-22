from .auth import router as auth_router
from .dashboard import router as dashboard_router
from .natalidad import router as natalidad_router
from .mortalidad import router as mortalidad_router
from .analytics import router as analytics_router
from .users import router as users_router
from .upload import router as upload_router

__all__ = [
    "auth_router",
    "dashboard_router",
    "natalidad_router",
    "mortalidad_router",
    "analytics_router",
    "users_router",
    "upload_router",
]
