from fastapi import FastAPI, status
from pydantic import BaseModel
import uvicorn

#on crée l'application
app = FastAPI()

class HealthCheck(BaseModel): #modèle de réponse pour la vérification de santé
    status: str = "OK"

# route de santé de l'API
@app.get(
    "/health",
    tags=["health"],
    summary="Vérifie si l'API fonctionne",
    response_model=HealthCheck,
    status_code=status.HTTP_200_OK,
)
def get_health(): 
    return HealthCheck(status="OK")

#pour lancer le serveur si on exécute directement lee fichier
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
