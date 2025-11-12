import json

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse

from pymongo.database import Database
from bson import ObjectId

from api.tags import APITags
from api.db import get_db

from flow_graph.runner import Runner

router = APIRouter(
    prefix="/flows",
    tags=[APITags.FLOWS],
)

################################################################################
# CRUD Methods
################################################################################

router = APIRouter(
    prefix="/flows",
    tags=[APITags.FLOWS],
)

COLLECTION_NAME = "flows"

################################################################################
# CRUD Methods
################################################################################

router = APIRouter(
    prefix="/flows",
    tags=[APITags.FLOWS],
)

COLLECTION_NAME = "flows"


@router.post("/")
async def create_flow(request: Request, db: Database = Depends(get_db)):
    try:
        payload = await request.json()
        result = db[COLLECTION_NAME].insert_one(payload)
        payload["_id"] = str(result.inserted_id)
        return JSONResponse({"status": "success", "data": payload})
    except Exception as e:
        print("Error creating flow :", e)
        return JSONResponse(
            {"status": "error", "message": "Failed to create flow"}, status_code=500
        )


@router.get("/")
async def get_all_flows(request: Request, db: Database = Depends(get_db)):
    try:
        flows = list(db[COLLECTION_NAME].find())
        for flow in flows:
            flow["_id"] = str(flow["_id"])
        return JSONResponse({"status": "success", "data": flows})
    except Exception as e:
        print("Error fetching flows :", e)
        return JSONResponse(
            {"status": "error", "message": "Failed to fetch flows"}, status_code=500
        )


@router.get("/{flow_uid}")
async def get_flow(request: Request, flow_uid: str, db: Database = Depends(get_db)):
    try:
        flow = db[COLLECTION_NAME].find_one({"_id": ObjectId(flow_uid)})
        if not flow:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )
        flow["_id"] = str(flow["_id"])
        return JSONResponse({"status": "success", "data": flow})
    except Exception as e:
        print("Error fetching flow :", e)
        return JSONResponse(
            {"status": "error", "message": "Invalid flow UID"}, status_code=500
        )


@router.put("/{flow_uid}")
async def update_flow(request: Request, flow_uid: str, db: Database = Depends(get_db)):
    try:
        payload = await request.json()
        result = db[COLLECTION_NAME].update_one(
            {"_id": ObjectId(flow_uid)}, {"$set": payload}
        )
        if result.matched_count == 0:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )
        payload["_id"] = flow_uid
        return JSONResponse({"status": "success", "data": payload})
    except Exception as e:
        print("Error updating flow :", e)
        return JSONResponse(
            {"status": "error", "message": "Invalid flow UID"}, status_code=500
        )


@router.delete("/{flow_uid}")
async def delete_flow(flow_uid: str, db: Database = Depends(get_db)):
    try:
        result = db[COLLECTION_NAME].delete_one({"_id": ObjectId(flow_uid)})
        if result.deleted_count == 0:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )
        return JSONResponse({"status": "success", "message": "Flow deleted"})
    except Exception as e:
        print("Error deleting flow :", e)
        return JSONResponse(
            {"status": "error", "message": "Invalid flow UID"}, status_code=500
        )


################################################################################
# Execution Methods
################################################################################


# In builder ... so no flow_uid
@router.post("/execute")
async def execute_flow(
    request: Request,
    db: Database = Depends(get_db),
    stream: bool = Query(default=False),
):
    try:
        payload = await request.json()
        flow_graph = payload.get("flow_graph")

        if not flow_graph:
            return JSONResponse(
                {"status": "error", "message": "Flow graph is empty"}, status_code=200
            )

        runner = Runner(payload)

        if stream:
            return StreamingResponse(
                runner.execute(),
                media_type="application/json",
                status_code=200,
            )

        # If not streaming, collect all outputs into a list
        outputs = list(runner.execute())
        # Convert JSON strings back to Python objects
        outputs = [json.loads(o) for o in outputs]

        return JSONResponse({"status": "success", "data": outputs}, status_code=200)

    except Exception as e:
        print("Error executing flow:", e)
        return JSONResponse(
            {"status": "error", "message": "Failed to execute flow", "detail": str(e)},
            status_code=500,
        )


@router.post("/execute/{flow_uid}")
async def execute_flow_by_flow_uid(
    request: Request,
    flow_uid: str,
    stream: bool = Query(default=False),
    db: Database = Depends(get_db),
):
    try:
        flow_graph_str = db[COLLECTION_NAME].find_one({"_id": ObjectId(flow_uid)})
        if not flow_graph_str:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )

        runner = Runner(flow_graph_str)

        if stream:
            return StreamingResponse(
                runner.execute(),
                media_type="application/json",
                status_code=200,
            )

        # If not streaming, collect all outputs into a list
        outputs = list(runner.execute())
        # Convert JSON strings back to Python objects
        outputs = [json.loads(o) for o in outputs]

        return JSONResponse({"status": "success", "data": outputs}, status_code=200)

    except Exception as e:
        print("Error executing flow:", e)
        return JSONResponse(
            {"status": "error", "message": "Failed to execute flow", "detail": str(e)},
            status_code=500,
        )
