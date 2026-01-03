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


# ---- helpers ----
def safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def get_field(fields: dict, field_name: str) -> dict:
    # DI fields: {"InvoiceId": {...}, ...}
    if not isinstance(fields, dict):
        return {}
    v = fields.get(field_name)
    return v if isinstance(v, dict) else {}


def get_confidence(field: dict):
    c = field.get("confidence")
    return c if isinstance(c, (int, float)) else None


def get_value_string(field: dict):
    # invoice id, tax id, recipients typically valueString
    return field.get("valueString") or ""


def get_value_date(field: dict):
    # dates typically valueDate (YYYY-MM-DD)
    return field.get("valueDate") or ""


def get_value_currency_amount(field: dict):
    return safe_get(field, "valueCurrency", "amount", default="") or ""


def get_value_currency_code(field: dict):
    return safe_get(field, "valueCurrency", "currencyCode", default="") or ""


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

    headers = [
        "InvoiceId",
        "InvoiceId_confidence",
        "VendorAddressRecipient",
        "VendorAddressRecipient_confidence",
        "VendorTaxId",
        "VendorTaxId_confidence",
        "CustomerAddressRecipient",
        "CustomerAddressRecipient_confidence",
        "CustomerTaxId",
        "CustomerTaxId_confidence",
        "InvoiceDate",
        "InvoiceDate_confidence",
        "DueDate",
        "DueDate_confidence",
        "InvoiceTotal_amount",
        "InvoiceTotal_currencyCode",
        "InvoiceTotal_confidence",
        "SubTotal_amount",
        "SubTotal_currencyCode",
        "SubTotal_confidence",
        "TotalTax_amount",
        "TotalTax_currencyCode",
        "TotalTax_confidence",
    ]

    rows = []

    for blob in container.list_blobs():
        if not blob.name.lower().endswith(".json"):
            continue

        raw = container.get_blob_client(blob.name).download_blob().readall()
        doc = json.loads(raw)

        documents = safe_get(doc, "analyzeResult", "documents", default=[])
        if not documents:
            # ha valamiért nincs documents tömb, akkor kihagyjuk
            continue

        fields = documents[0].get("fields") or {}

        # string fields
        invoice_id = get_field(fields, "InvoiceId")
        vendor_addr_rec = get_field(fields, "VendorAddressRecipient")
        vendor_tax = get_field(fields, "VendorTaxId")
        cust_addr_rec = get_field(fields, "CustomerAddressRecipient")
        cust_tax = get_field(fields, "CustomerTaxId")

        # date fields
        invoice_date = get_field(fields, "InvoiceDate")
        due_date = get_field(fields, "DueDate")

        # currency fields
        invoice_total = get_field(fields, "InvoiceTotal")
        sub_total = get_field(fields, "SubTotal")
        total_tax = get_field(fields, "TotalTax")

        row = [
            get_value_string(invoice_id),
            get_confidence(invoice_id) or "",
            get_value_string(vendor_addr_rec),
            get_confidence(vendor_addr_rec) or "",
            get_value_string(vendor_tax),
            get_confidence(vendor_tax) or "",
            get_value_string(cust_addr_rec),
            get_confidence(cust_addr_rec) or "",
            get_value_string(cust_tax),
            get_confidence(cust_tax) or "",
            get_value_date(invoice_date),
            get_confidence(invoice_date) or "",
            get_value_date(due_date),
            get_confidence(due_date) or "",
            get_value_currency_amount(invoice_total),
            get_value_currency_code(invoice_total),
            get_confidence(invoice_total) or "",
            get_value_currency_amount(sub_total),
            get_value_currency_code(sub_total),
            get_confidence(sub_total) or "",
            get_value_currency_amount(total_tax),
            get_value_currency_code(total_tax),
            get_confidence(total_tax) or "",
        ]

        rows.append(row)

    if not rows:
        raise HTTPException(404, "No invoice JSON files found in result container")

    wb = Workbook()
    ws = wb.active
    ws.title = "Invoices"
    ws.append(headers)
    for r in rows:
        ws.append(r)

    out = BytesIO()
    wb.save(out)
    out.seek(0)

    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=invoices.xlsx"},
    )
