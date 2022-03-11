from fastapi import FastAPI
import uvicorn
from starlette.middleware.cors import CORSMiddleware

from src.rest.AuthRoute import authroute
from src.rest.PythiaRoute import pythiaroute

api = FastAPI()
#origins = ["http://localhost", "http://localhost:8080", "http://localhost:4200"]
origins = ["*"]
api.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api.include_router(authroute, prefix="/auth")
api.include_router(pythiaroute, prefix="/scenario")

if __name__ == '__main__':
    uvicorn.run(api, port=8080, host='127.0.0.1')


