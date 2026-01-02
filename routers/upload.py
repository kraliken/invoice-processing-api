import base64
import re
import unicodedata
from datetime import datetime
import os
from fastapi import APIRouter, HTTPException, Request, status
from azure.storage.blob import BlobServiceClient, ContentSettings

router = APIRouter(prefix="/upload", tags=["upload"])

CONTAINER_NAME = "invoicebatch"


def get_blob_service_client() -> BlobServiceClient:
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING nincs beállítva.")
    return BlobServiceClient.from_connection_string(conn_str)


def slugify_filename(name: str) -> str:
    # levágjuk az esetleges útvonalat
    name = name.split("\\")[-1].split("/")[-1]

    # Unicode normalizálás + ékezetek eldobása
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")

    # kényes karakterek -> _
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)

    # ne legyen nagyon csúnya: több _ egymás után
    name = re.sub(r"_+", "_", name).strip("._-")

    return name or "file"


@router.post("/invoice")
async def upload_invoice(request: Request):

    data = await request.json()

    file_name = data.get("fileName") or "invoice.pdf"
    content_type = data.get("contentType") or "application/pdf"
    content_b64 = data.get("contentBase64")

    if not content_b64:
        raise HTTPException(status_code=400, detail="Hiányzik a 'contentBase64' mező.")
    if content_type != "application/pdf":
        raise HTTPException(
            status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
            detail="Csak PDF fájl feltöltése engedélyezett.",
        )

    if "," in content_b64:
        content_b64 = content_b64.split(",", 1)[1]

    try:
        file_bytes = base64.b64decode(content_b64, validate=True)
    except Exception:
        raise HTTPException(
            status_code=400, detail="A 'contentBase64' nem érvényes base64."
        )

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_name = slugify_filename(file_name)
    blob_name = f"{ts}_{safe_name}"

    try:
        bsc = get_blob_service_client()
        container = bsc.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(blob_name)

        blob.upload_blob(
            file_bytes,
            overwrite=False,
            content_settings=ContentSettings(content_type=content_type),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Blob feltöltés hiba: {e}")

    return {
        "ok": True,
        "container": CONTAINER_NAME,
        "blobName": blob_name,
        "size": len(file_bytes),
    }
