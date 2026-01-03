import os
import secrets
from fastapi.responses import StreamingResponse
from azure.storage.blob import BlobServiceClient
import httpx
from fastapi import APIRouter, HTTPException, Request
from openpyxl import Workbook
from io import BytesIO
import json

from urllib.parse import urlparse

router = APIRouter(prefix="/docint", tags=["docint"])


def get_container_url(account: str, container: str) -> str:
    """
    Storage account + konténer névből összeállít egy konténer URL-t.
    Pl.: https://mystorage.blob.core.windows.net/invoicebatch
    """
    return f"https://{account}.blob.core.windows.net/{container}"


def extract_result_id(operation_location: str) -> str:
    """
    A Document Intelligence analyzeBatch válaszában a resultId-t
    tipikusan az `operation-location` header URL utolsó path eleme tartalmazza.

    Pl.: .../documentModels/prebuilt-invoice/analyzeResults/<RESULT_ID>
    """
    try:
        path = urlparse(operation_location).path
        return path.rstrip("/").split("/")[-1]
    except:
        return ""


def require_flow_secret(request: Request):
    """
    "Jelképes" védelem: a Flow küld egy x-flow-secret headert,
    mi pedig összevetjük egy szerver oldali környezeti változóval.

    - FLOW_SHARED_SECRET: App Service Application settings-ben legyen beállítva
    - x-flow-secret: Power Automate HTTP headerben küldöd
    """
    expected = os.getenv("FLOW_SHARED_SECRET", "")

    # Ha nincs beállítva szerveren a shared secret, akkor ez konfigurációs hiba:
    if not expected:
        raise HTTPException(500, "FLOW_SHARED_SECRET nincs beállítva a szerveren.")

    # A kérésből kiolvassuk a headert:
    provided = request.headers.get("x-flow-secret", "")

    # Timing-safe összehasonlítás (ne lehessen időzítés alapján tippelni):
    if not provided or not secrets.compare_digest(provided, expected):
        raise HTTPException(401, "Unauthorized")


def extract_field_value(field: dict):
    """
    Document Intelligence field -> emberi érték
    """
    if not field:
        return None
    for key in ("content", "valueString", "valueNumber", "valueDate"):
        if key in field:
            return field[key]
    return None


@router.post("/batch/start")
async def start_invoice_batch(request: Request):
    """
    Batch feldolgozás indítása a Document Intelligence analyzeBatch API-val.

    Várt kérés (Flow-ból):
    - Header: x-flow-secret: <shared secret>
    """

    # 1) Egyszerű védelem: ha nincs / hibás a secret, azonnal leállunk
    require_flow_secret(request)

    # 4) Konfiguráció beolvasása környezeti változókból
    endpoint = (os.getenv("DOCINT_ENDPOINT") or "").rstrip("/")
    key = os.getenv("DOCINT_KEY") or ""
    account = os.getenv("AZURE_STORAGE_ACCOUNT_NAME") or ""

    # input és output konténerek (defaults)
    source_container = os.getenv("AZURE_STORAGE_SOURCE_CONTAINER") or "invoicebatch"
    result_container = (
        os.getenv("AZURE_STORAGE_RESULT_CONTAINER") or "invoicebatch-result"
    )

    # Ha hiányzik bármi alap, akkor konfigurációs hiba:
    if not endpoint or not key or not account:
        raise HTTPException(
            500, "Hiányzó DOCINT_ENDPOINT / DOCINT_KEY / AZURE_STORAGE_ACCOUNT_NAME."
        )

    # 5) Document Intelligence analyzeBatch paraméterek
    api_version = "2024-11-30"
    model_id = "prebuilt-invoice"

    # 6) A batch analyze URL összeállítása
    url = f"{endpoint}/documentintelligence/documentModels/{model_id}:analyzeBatch?api-version={api_version}"

    # 7) Request body összeállítása a DI-hoz
    # - azureBlobSource.containerUrl: input konténer URL
    # - azureBlobSource.prefix: csak akkor tesszük bele, ha van prefix
    # - resultContainerUrl: output konténer URL
    # - overwriteExisting: felülírja a meglévő result fájlokat ugyanazzal a prefixxel
    body = {
        "azureBlobSource": {
            "containerUrl": get_container_url(account, source_container),
        },
        "resultContainerUrl": get_container_url(account, result_container),
        "overwriteExisting": True,
    }

    # 8) HTTP hívás a Document Intelligence felé
    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.post(
            url,
            headers={
                "Ocp-Apim-Subscription-Key": key,
                "Content-Type": "application/json",
            },
            json=body,
        )

    # 9) Hibakezelés: 2xx-on kívül mindent hibának veszünk
    if res.status_code < 200 or res.status_code >= 300:
        detail = await res.aread()
        raise HTTPException(
            res.status_code,
            f"Batch indítás hiba: {detail.decode('utf-8', 'ignore')[:500]}",
        )

    # 10) Siker esetén a DI egy operation-location headert ad vissza,
    #     ebből ki tudjuk venni a resultId-t
    operation_location = res.headers.get("operation-location", "")
    result_id = extract_result_id(operation_location)

    # 11) Visszaadunk egy flow-barát JSON választ
    return {
        "ok": True,
        "operationLocation": operation_location,
        "resultId": result_id,
        "sourceContainer": source_container,
        "resultContainer": result_container,
        "docIntRequest": body,
    }


@router.get("/export/excel")
def export_invoices_to_excel():

    RESULT_CONTAINER = os.getenv(
        "AZURE_STORAGE_RESULT_CONTAINER", "invoicebatch-result"
    )
    CONN_STR = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    if not CONN_STR:
        raise HTTPException(500, "AZURE_STORAGE_CONNECTION_STRING not set")

    bsc = BlobServiceClient.from_connection_string(CONN_STR)
    container = bsc.get_container_client(RESULT_CONTAINER)

    rows = []
    all_columns = set()

    # 1) Összes JSON blob beolvasása
    for blob in container.list_blobs():
        if not blob.name.lower().endswith(".json"):
            continue

        data = container.get_blob_client(blob.name).download_blob().readall()
        doc = json.loads(data)

        documents = doc.get("analyzeResult", {}).get("documents", [])
        if not documents:
            continue

        fields = documents[0].get("fields", {})
        row = {}

        for field_name, field_value in fields.items():
            value = extract_field_value(field_value)
            row[field_name] = value
            all_columns.add(field_name)

        rows.append(row)

    if not rows:
        raise HTTPException(404, "No invoice JSON files found")

    # 2) Excel összeállítása
    wb = Workbook()
    ws = wb.active
    ws.title = "Invoices"

    columns = sorted(all_columns)
    ws.append(columns)

    for row in rows:
        ws.append([row.get(col) for col in columns])

    # 3) Excel stream visszaadása
    output = BytesIO()
    wb.save(output)
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=invoices.xlsx"},
    )
