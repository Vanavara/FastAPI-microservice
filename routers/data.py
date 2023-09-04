# third-party
from fastapi import UploadFile, File, APIRouter

# project
from services.data_service import process_upload_internal, fetch_and_aggregate_data_internal


data_router = APIRouter(tags=["1. DATA"], prefix="/data")


@data_router.post("/upload")
async def process_upload(file: UploadFile = File(...)):
    return process_upload_internal(file)


@data_router.get("/download/")
async def fetch_and_aggregate_data(version: int):
    return fetch_and_aggregate_data_internal(version)
