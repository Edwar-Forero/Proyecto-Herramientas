from .auth_service import auth_service
from .dashboard_service import dashboard_service
from .natalidad_service import natalidad_service
from .mortalidad_service import mortalidad_service
from .analytics_service import analytics_service
from .prediction_service import prediction_service
from .user_service import user_service
from .upload_service import upload_service

__all__ = [
    "auth_service",
    "dashboard_service",
    "natalidad_service",
    "mortalidad_service",
    "analytics_service",
    "prediction_service",
    "user_service",
    "upload_service",
]
