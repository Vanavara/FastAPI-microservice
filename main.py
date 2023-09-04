# third-party
from fastapi import FastAPI, APIRouter
from fastapi.openapi.utils import get_openapi
from starlette.responses import RedirectResponse

# project
from routers.data import data_router


# the following settings will help with API scalability


# Define a root router
root_router = APIRouter(prefix="/api/v1")

# Define an admin router
admin_router = APIRouter(prefix="/admin")

# Define a media router
media_router = APIRouter(prefix="/media")

# Define a FastAPI application
app = FastAPI(title="Data API", version="0.0.1")


# 1. DATA
app.include_router(data_router)

# Connect admin sub-routers to admin router
root_router.include_router(admin_router)

# Connect a root router to an application
app.include_router(root_router)
app.include_router(media_router)


# Redirect root URL to /docs
@app.get("/")
async def root():
    return RedirectResponse(url="/docs")

def openapi_specs():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Data",
        version="0.0.1",
        description="Data Open-API Specification",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = openapi_specs
