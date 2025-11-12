import json

import yaml
from fastapi import FastAPI, Response

OPENAPI_VERSION = "3.0.0"


def render_yaml_spec(app: FastAPI):
    if not app.openapi_schema:
        app.openapi()

    app.openapi_version = OPENAPI_VERSION
    schema = app.openapi_schema.copy()

    clean_schema(schema)

    yaml_str = yaml.dump(schema, sort_keys=False, default_flow_style=False, indent=2)

    return Response(content=yaml_str, media_type="text/yaml")


def render_json_spec(app: FastAPI):
    if not app.openapi_schema:
        app.openapi()

    app.openapi_version = OPENAPI_VERSION
    schema = app.openapi_schema.copy()

    clean_schema(schema)

    schema = json.dumps(schema, indent=2)

    return Response(content=schema, media_type="application/json")


def clean_schema(obj):
    """Recursively clean OpenAPI schema:
    - remove 'examples'
    - remove 'type: null'
    - fix 'anyOf' with type: 'null'
    """
    if isinstance(obj, dict):
        obj.pop("examples", None)

        # Fix `anyOf`: remove items where type == 'null'
        if "anyOf" in obj:
            filtered = []
            for entry in obj["anyOf"]:
                if isinstance(entry, dict) and entry.get("type") == "null":
                    continue
                filtered.append(entry)
            obj["anyOf"] = filtered
            if len(obj["anyOf"]) == 1:
                # Flatten single-item anyOf
                obj.update(obj["anyOf"][0])
                obj.pop("anyOf")

        # Remove plain 'type: null'
        if obj.get("type") == "null":
            obj.pop("type")

        for key, value in list(obj.items()):
            clean_schema(value)

    elif isinstance(obj, list):
        for item in obj:
            clean_schema(item)
