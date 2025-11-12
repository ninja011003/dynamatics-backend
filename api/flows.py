import json

from fastapi import APIRouter, Depends, Request, Query, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from pymongo.database import Database

from api.tags import APITags
from api.db import get_db

from flow_graph.runner import Runner

from api.utils import generate_uid

router = APIRouter(
    prefix="/flows",
    tags=[APITags.FLOWS],
)

COLLECTION_NAME = "flows"


################################################################################
# CRUD Methods
################################################################################


@router.post("")
async def create_flow(request: Request, db: Database = Depends(get_db)):
    try:
        flow_uid = generate_uid("flow", 18)
        payload = await request.json()
        payload["flow_uid"] = flow_uid
        db[COLLECTION_NAME].insert_one(payload)
        return JSONResponse({"status": "success", "data": flow_uid})
    except Exception as e:
        print("Error creating flow :", e)
        return JSONResponse(
            {"status": "error", "message": "Failed to create flow"}, status_code=500
        )


@router.get("")
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
        flow = db[COLLECTION_NAME].find_one({"flow_uid": flow_uid})
        flow["_id"] = str(flow["_id"])
        if not flow:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )
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
            {"flow_uid": flow_uid}, {"$set": payload}
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
        result = db[COLLECTION_NAME].delete_one({"flow_uid": flow_uid})
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
    return_data: bool = Query(default=True),
):
    try:
        payload = await request.json()
        flow_graph = payload.get("flow_graph")

        if not flow_graph:
            return JSONResponse(
                {"status": "error", "message": "Flow graph is empty"}, status_code=200
            )

        runner = Runner(flow_graph)

        if stream and return_data:
            return StreamingResponse(
                runner.execute(),
                media_type="application/json",
                status_code=200,
            )

        outputs = list(runner.execute())
        print("\n\noutputs : ", outputs)

        outputs = [json.loads(o) for o in outputs]
        if not return_data:
            metadata = []
            for output in outputs:
                metadata.append(
                    {
                        "node_id": output["node_id"],
                        "total_rows": len(output["output"]),
                        "column_names": list(output["output"][0].keys()),
                        "column_types": [
                            "string" if isinstance(x, str) else "number"
                            for x in output["output"][0].values()
                        ],
                    }
                )
            return JSONResponse(
                {"status": "success", "data": metadata}, status_code=200
            )

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
        data = db[COLLECTION_NAME].find_one({"flow_uid": flow_uid})

        if not data:
            return JSONResponse(
                {"status": "error", "message": "Flow not found"}, status_code=200
            )

        flow_graph_dict = data.get("flow_graph", "")
        runner = Runner(flow_graph_dict)

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


################################################################################
# Metadata Methods
################################################################################


@router.get("/metadata/{dataset_name}")
async def get_data_node_metadata(
    request: Request, dataset_name: str, db: Database = Depends(get_db)
):
    # Static metadata dictionary for all mock datasets
    DATASET_METADATA = {
        "timeseries": {
            "total_rows": 62,
            "column_names": ["date", "cost", "revenue", "product"],
            "column_types": ["str", "float", "float", "str"]
        },
        "timeseries_long": {
            "total_rows": 365,
            "column_names": ["date", "cost", "revenue", "product"],
            "column_types": ["str", "float", "float", "str"]
        },
        "timeseries_multi": {
            "total_rows": 270,
            "column_names": ["date", "cost", "revenue", "product"],
            "column_types": ["str", "float", "float", "str"]
        },
        "automate": {
            "total_rows": 7000,
            "column_names": [
                "type", "timestamp", "stack_uid", "organization_uid", "user_uid",
                "net.client.ip", "net.client.port", "net.client.user_agent",
                "net.server.ip", "net.server.port", "net.server.hostname",
                "http.request.method", "http.request.url", "http.request.headers.user-agent",
                "http.request.headers.accept", "http.request.headers.x-request-id",
                "http.request.query_params", "http.request.body",
                "http.response.status_code", "http.response.body", "http.response.errors",
                "metrics.response_time_ms",
                "automate.api_requests", "automate.executions"
            ],
            "column_types": [
                "str", "str", "str", "str", "str",
                "str", "int", "str",
                "str", "int", "str",
                "str", "str", "str",
                "str", "str",
                "dict", "str",
                "int", "str", "str",
                "int",
                "list", "list"
            ]
        },
        "brandkit": {
            "total_rows": 10000,
            "column_names": [
                "type", "timestamp", "stack_uid", "organization_uid", "user_uid",
                "net.client.ip", "net.client.port", "net.client.user_agent",
                "net.server.ip", "net.server.port", "net.server.hostname",
                "http.request.method", "http.request.url", "http.request.headers.user-agent",
                "http.request.headers.accept", "http.request.headers.x-request-id",
                "http.request.query_params", "http.request.body",
                "http.response.status_code", "http.response.body", "http.response.errors",
                "metrics.response_time_ms",
                "brandkit.voice_profiles", "brandkit.api_requests", "brandkit.brand_kits"
            ],
            "column_types": [
                "str", "str", "str", "str", "str",
                "str", "int", "str",
                "str", "int", "str",
                "str", "str", "str",
                "str", "str",
                "dict", "str",
                "int", "str", "str",
                "int",
                "list", "list", "list"
            ]
        },
        "cms": {
            "total_rows": 15000,
            "column_names": [
                "type", "timestamp", "stack_uid", "organization_uid", "user_uid",
                "net.client.ip", "net.client.port", "net.client.user_agent",
                "net.server.ip", "net.server.port", "net.server.hostname",
                "http.request.method", "http.request.url", "http.request.headers.user-agent",
                "http.request.headers.accept", "http.request.headers.x-request-id",
                "http.request.query_params", "http.request.body",
                "http.response.status_code", "http.response.body", "http.response.errors",
                "metrics.response_time_ms",
                "cms.assets", "cms.api_requests", "cms.content_types"
            ],
            "column_types": [
                "str", "str", "str", "str", "str",
                "str", "int", "str",
                "str", "int", "str",
                "str", "str", "str",
                "str", "str",
                "dict", "str",
                "int", "str", "str",
                "int",
                "list", "list", "list"
            ]
        },
        "launch": {
            "total_rows": 4000,
            "column_names": [
                "type", "timestamp", "stack_uid", "organization_uid", "user_uid",
                "net.client.ip", "net.client.port", "net.client.user_agent",
                "net.server.ip", "net.server.port", "net.server.hostname",
                "http.request.method", "http.request.url", "http.request.headers.user-agent",
                "http.request.headers.accept", "http.request.headers.x-request-id",
                "http.request.query_params", "http.request.body",
                "http.response.status_code", "http.response.body", "http.response.errors",
                "metrics.response_time_ms",
                "launch.environments"
            ],
            "column_types": [
                "str", "str", "str", "str", "str",
                "str", "int", "str",
                "str", "int", "str",
                "str", "str", "str",
                "str", "str",
                "dict", "str",
                "int", "str", "str",
                "int",
                "list"
            ]
        },
        "test": {
            "total_rows": 17,
            "column_names": ["id", "name", "age", "country", "salary"],
            "column_types": ["int", "str", "int", "str", "int"]
        },
        "test2": {
            "total_rows": 11,
            "column_names": ["id", "department", "budget"],
            "column_types": ["int", "str", "int"]
        }
    }
    
    # Check if dataset exists
    if dataset_name not in DATASET_METADATA:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
    
    return DATASET_METADATA[dataset_name]
