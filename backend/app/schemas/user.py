from pydantic import BaseModel, Field


class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6, max_length=128)
    rol: str = Field(..., pattern="^(admin|analista|consulta)$")


class UserOut(BaseModel):
    id: str
    username: str
    rol: str
    rol_label: str
    activo: bool


class UserListResponse(BaseModel):
    users: list[UserOut]
    total: int
