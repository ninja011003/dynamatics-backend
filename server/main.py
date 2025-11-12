from http import HTTPStatus
from contextlib import asynccontextmanager


from dotenv import load_dotenv
from pymongo import MongoClient

from fastapi import APIRouter, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from middlewares.security import SecurityHeadersMiddleware

from api.flows.router import router as FlowsApiRouter
from api.dashboard.router import router as DashboardApiRouter

from api.tags import APITags

load_dotenv()

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "dynamatics-backend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.mongo_client = MongoClient(MONGO_URI)
    print("MongoDB connected!")

    yield

    app.state.mongo_client.close()
    print("MongoDB connection closed !")


app = FastAPI(
    lifespan=lifespan,
    title="Dynamatics Backend",
    version="3.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url="/openapi.json",
)

app.openapi_version = "3.0.0"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.add_middleware(SecurityHeadersMiddleware)


api_router = APIRouter()

api_router.include_router(FlowsApiRouter)
api_router.include_router(DashboardApiRouter)

app.include_router(api_router, prefix="/api")


@app.get("/", tags=[APITags.HEALTH_CHECK])
@app.get("/health_check", tags=[APITags.HEALTH_CHECK])
async def health_check():
    return {"status": HTTPStatus.OK, "message": "ok"}
