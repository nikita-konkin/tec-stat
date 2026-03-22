"""
Utilities for exporting API payloads as JSON, CSV, or XLSX.

JSON responses keep the original payload structure.
CSV/XLSX responses flatten nested objects/lists into tabular rows with
column names so data can be opened directly in spreadsheets.
"""

from __future__ import annotations

import io
from typing import Any, Literal

import pandas as pd
from fastapi.encoders import jsonable_encoder
from fastapi.responses import StreamingResponse

ExportFormat = Literal["json", "csv", "xlsx"]


def format_payload(payload: Any, fmt: ExportFormat, filename_base: str) -> Any:
    """
    Return payload as JSON (original) or downloadable CSV/XLSX.
    """
    if fmt == "json":
        return payload

    df = payload_to_dataframe(payload)

    if fmt == "csv":
        csv_text = df.to_csv(index=False)
        data = csv_text.encode("utf-8-sig")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename_base}.csv"'
            },
        )

    xlsx_buffer = io.BytesIO()
    with pd.ExcelWriter(xlsx_buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="data")
    xlsx_buffer.seek(0)

    return StreamingResponse(
        xlsx_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f'attachment; filename="{filename_base}.xlsx"'
        },
    )


def payload_to_dataframe(payload: Any) -> pd.DataFrame:
    """
    Flatten API payload into a tabular DataFrame with explicit column names.
    """
    data = jsonable_encoder(payload)
    rows = _flatten_rows(data, {})

    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows, sep="_")

    # Hide helper column when not needed.
    if "collection" in df.columns and df["collection"].isna().all():
        df = df.drop(columns=["collection"])

    return df


def _flatten_rows(value: Any, context: dict[str, Any]) -> list[dict[str, Any]]:
    if _is_scalar(value):
        row = dict(context)
        row["value"] = value
        return [row]

    if isinstance(value, list):
        rows: list[dict[str, Any]] = []
        for item in value:
            rows.extend(_flatten_rows(item, context))
        return rows

    if isinstance(value, dict):
        scalars: dict[str, Any] = {}
        nested_dicts: dict[str, dict[str, Any]] = {}
        nested_lists: dict[str, list[Any]] = {}

        for key, val in value.items():
            if _is_scalar(val):
                scalars[key] = val
            elif isinstance(val, dict):
                nested_dicts[key] = val
            elif isinstance(val, list):
                nested_lists[key] = val
            else:
                scalars[key] = str(val)

        base = dict(context)
        base.update(scalars)

        for key, val in nested_dicts.items():
            for sub_key, sub_val in val.items():
                col = f"{key}_{sub_key}"
                base[col] = sub_val if _is_scalar(sub_val) else str(sub_val)

        if not nested_lists:
            return [base]

        rows: list[dict[str, Any]] = []
        multi_collection = len(nested_lists) > 1

        for list_key, list_value in nested_lists.items():
            if not list_value:
                continue

            if all(_is_scalar(item) for item in list_value):
                item_col = _singularize(list_key)
                for item in list_value:
                    row = dict(base)
                    if multi_collection:
                        row["collection"] = list_key
                    row[item_col] = item
                    rows.append(row)
                continue

            for item in list_value:
                child_context = dict(base)
                if multi_collection:
                    child_context["collection"] = list_key
                rows.extend(_flatten_rows(item, child_context))

        return rows if rows else [base]

    row = dict(context)
    row["value"] = str(value)
    return [row]


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _singularize(name: str) -> str:
    if name.endswith("ies") and len(name) > 3:
        return name[:-3] + "y"
    if name.endswith("s") and len(name) > 1:
        return name[:-1]
    return f"{name}_item"
