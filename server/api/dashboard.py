from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from pymongo.database import Database

from api.tags import APITags

from db import get_db

router = APIRouter(
    prefix="/dashboard",
    tags=[APITags.FLOWS],
)

################################################################################
# CRUD Methods
################################################################################


@router.get("/adv-analytics")
async def get_all_flows_in_dashboard(
    request: Request,
    db: Database = Depends(get_db),
):
    pass


@router.get("/")
async def get_flow_in_dashboard(
    request: Request,
    db: Database = Depends(get_db),
):
    pass


@router.get("/{flow_uid}")
async def get_flow_by_flow_uid_in_dashboard(
    flow_uid: str,
    request: Request,
    db: Database = Depends(get_db),
):
    pass
