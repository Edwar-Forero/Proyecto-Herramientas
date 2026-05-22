import asyncio
import os
import sys
from pathlib import Path

# Añadir el path del backend para poder importar de app
backend_path = Path(__file__).resolve().parents[2] / "backend"
sys.path.insert(0, str(backend_path))

from app.database.mongodb import mongodb
from app.models.collections import USUARIOS
from app.utils.security import hash_password

async def setup():
    print("Conectando a MongoDB...")
    mongodb.connect()
    db = mongodb.get_database()
    
    admin_exists = await db[USUARIOS].find_one({"username": "admin"})
    if admin_exists:
        print("El usuario admin ya existe.")
    else:
        print("Creando usuario admin inicial...")
        await db[USUARIOS].insert_one({
            "username": "admin",
            "password_hash": hash_password("admin123"),
            "rol": "admin",
            "activo": True
        })
        print("Usuario admin creado exitosamente. (Username: admin, Password: admin123)")
    
    # También podemos crear un analista y consulta de prueba
    analista_exists = await db[USUARIOS].find_one({"username": "analista"})
    if not analista_exists:
        print("Creando usuario analista inicial...")
        await db[USUARIOS].insert_one({
            "username": "analista",
            "password_hash": hash_password("analista123"),
            "rol": "analista",
            "activo": True
        })
        
    consulta_exists = await db[USUARIOS].find_one({"username": "consulta"})
    if not consulta_exists:
        print("Creando usuario consulta inicial...")
        await db[USUARIOS].insert_one({
            "username": "consulta",
            "password_hash": hash_password("consulta123"),
            "rol": "consulta",
            "activo": True
        })
        
    print("Terminado.")
    await mongodb.disconnect()

if __name__ == "__main__":
    asyncio.run(setup())
