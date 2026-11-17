"""FastAPI 网页路由：上传、异常更正、读数查看、两月发布、版本与下载。"""
from __future__ import annotations

import csv
import io
from contextlib import asynccontextmanager
from urllib.parse import quote

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates

from . import config
from .database import close_conn, get_conn, init_db
from .services import (
    PublishError,
    UploadError,
    compute_intervals,
    correct_readings,
    publish_two_months,
    readings_state_hash,
    upload_readings,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="园区总表用量发布平台", lifespan=lifespan)
templates = Jinja2Templates(directory=config.BASE_DIR + "/app/templates")


# ---------------------------------------------------------------- 辅助查询

def _dashboard_data(conn):
    stats = {
        "readings": conn.execute("SELECT COUNT(*) c FROM readings").fetchone()["c"],
        "batches": conn.execute("SELECT COUNT(*) c FROM upload_batches").fetchone()["c"],
        "versions": conn.execute("SELECT COUNT(*) c FROM published_versions").fetchone()["c"],
        "rejected": conn.execute(
            "SELECT COUNT(*) c FROM upload_batches WHERE status='rejected'").fetchone()["c"],
        "non_finite": conn.execute(
            "SELECT COUNT(*) c FROM readings"
            " WHERE cumulative NOT BETWEEN -1e308 AND 1e308").fetchone()["c"],
        "corrections": conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"],
    }
    latest = conn.execute(
        "SELECT * FROM published_versions ORDER BY id DESC LIMIT 1").fetchone()
    recent_batches = conn.execute(
        "SELECT * FROM upload_batches ORDER BY id DESC LIMIT 8").fetchall()
    return stats, latest, recent_batches


def _non_finite_reading_count(conn) -> int:
    # SQLite 没有 isfinite；REAL  Infinity 超出双精度有限范围，用边界近似识别，
    # 同时兼容驱动把 NaN 读成 NULL 的情形（readings.cumulative NOT NULL，实际仅 inf）。
    return conn.execute(
        "SELECT COUNT(*) c FROM readings"
        " WHERE cumulative IS NULL OR cumulative NOT BETWEEN -1e308 AND 1e308"
    ).fetchone()["c"]


# ---------------------------------------------------------------- 首页

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    conn = get_conn()
    try:
        stats, latest, recent_batches = _dashboard_data(conn)
        return templates.TemplateResponse(request, "home.html", {
            "stats": stats,
            "latest": latest, "recent_batches": recent_batches,
        })
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 普通上传

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request):
    conn = get_conn()
    try:
        batches = conn.execute(
            "SELECT * FROM upload_batches WHERE kind='upload'"
            " ORDER BY id DESC LIMIT 20").fetchall()
        non_finite_cnt = _non_finite_reading_count(conn)
        return templates.TemplateResponse(request, "upload.html", {
            "batches": batches, "non_finite_cnt": non_finite_cnt,
        })
    finally:
        close_conn(conn)


@app.post("/upload")
async def upload_csv(request: Request, file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename or "未命名文件"
    conn = get_conn()
    try:
        result = upload_readings(conn, filename, content)
        return RedirectResponse(
            f"/upload?ok=1&inserted={result['inserted']}&skipped={result['skipped']}",
            status_code=303)
    except UploadError as exc:
        # 解析/规则拒收：独立留痕，读数保持不变
        from .services import _record_rejected
        try:
            _record_rejected(filename, str(exc)[:500], kind="upload")
        except Exception:
            pass  # 留痕失败不掩盖原始拒收原因
        return RedirectResponse(
            "/upload?err=UploadError&msg=" + quote(str(exc)[:300]),
            status_code=303)
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 异常读数更正

@app.get("/corrections", response_class=HTMLResponse)
def corrections_page(request: Request):
    conn = get_conn()
    try:
        pending = conn.execute(
            "SELECT r.id, r.ts, r.period_no, r.cumulative, r.batch_id, b.filename"
            " FROM readings r JOIN upload_batches b ON b.id=r.batch_id"
            " WHERE r.cumulative IS NULL OR r.cumulative NOT BETWEEN -1e308 AND 1e308"
            " ORDER BY r.ts, r.period_no").fetchall()
        batches = conn.execute(
            "SELECT * FROM upload_batches WHERE kind='correction'"
            " ORDER BY id DESC LIMIT 20").fetchall()
        history = conn.execute(
            "SELECT rc.*, rb.filename AS correction_filename,"
            " ob.filename AS original_filename"
            " FROM reading_corrections rc"
            " JOIN upload_batches rb ON rb.id=rc.correction_batch_id"
            " JOIN upload_batches ob ON ob.id=rc.original_batch_id"
            " ORDER BY rc.id DESC LIMIT 50").fetchall()
        return templates.TemplateResponse(request, "corrections.html", {
            "pending": pending, "batches": batches, "history": history,
        })
    finally:
        close_conn(conn)


@app.post("/corrections")
async def submit_correction(request: Request, file: UploadFile = File(...)):
    content = await file.read()
    filename = file.filename or "未命名更正文件"
    conn = get_conn()
    try:
        result = correct_readings(conn, filename, content)
        return RedirectResponse(
            f"/corrections?ok=1&corrected={result['corrected']}",
            status_code=303)
    except UploadError as exc:
        # 更正拒收：仅留一条拒收批次痕跡，读数与 reading_corrections 不变
        from .services import _record_rejected
        try:
            _record_rejected(filename, str(exc)[:500], kind="correction")
        except Exception:
            pass
        return RedirectResponse(
            "/corrections?err=UploadError&msg=" + quote(str(exc)[:300]),
            status_code=303)
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 读数

@app.get("/readings", response_class=HTMLResponse)
def readings_page(request: Request):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT r.*, b.filename,"
            " (SELECT COUNT(*) FROM reading_corrections rc"
            "  WHERE rc.reading_id=r.id) AS corrected_times"
            " FROM readings r"
            " JOIN upload_batches b ON b.id = r.batch_id"
            " ORDER BY r.ts, r.period_no").fetchall()
        point_rows = conn.execute(
            "SELECT ts, period_no, cumulative FROM readings").fetchall()
        intervals = compute_intervals(point_rows)
        state_hash = readings_state_hash(point_rows)
        corrections = conn.execute(
            "SELECT rc.*, rb.filename AS correction_filename FROM reading_corrections rc"
            " JOIN upload_batches rb ON rb.id=rc.correction_batch_id"
            " ORDER BY rc.id").fetchall()
        return templates.TemplateResponse(request, "readings.html", {
            "rows": rows,
            "intervals": intervals, "state_hash": state_hash,
            "corrections": corrections,
        })
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 发布

@app.get("/publish", response_class=HTMLResponse)
def publish_page(request: Request):
    conn = get_conn()
    try:
        versions = conn.execute(
            "SELECT * FROM published_versions ORDER BY id DESC LIMIT 20").fetchall()
        non_finite_cnt = _non_finite_reading_count(conn)
        return templates.TemplateResponse(request, "publish.html", {
            "versions": versions, "non_finite_cnt": non_finite_cnt,
        })
    finally:
        close_conn(conn)


@app.post("/publish")
def publish_form(request: Request, month1: str = Form(...), month2: str = Form(...)):
    fail = bool(config.ENABLE_FAULT_INJECTION and
                request.headers.get("X-Fail-Publish") == "1")
    conn = get_conn()
    try:
        result = publish_two_months(conn, month1.strip(), month2.strip(),
                                    fail_before_commit=fail)
        return RedirectResponse(
            f"/versions/{result['version_id']}?published=1", status_code=303)
    except PublishError as exc:
        versions = conn.execute(
            "SELECT * FROM published_versions ORDER BY id DESC LIMIT 20").fetchall()
        return templates.TemplateResponse(request, "publish.html", {
            "versions": versions,
            "error": str(exc), "month1": month1, "month2": month2,
            "non_finite_cnt": _non_finite_reading_count(conn),
        }, status_code=400)
    except RuntimeError as exc:
        # 含自测故障注入：事务已回滚，不留新版本，按发布失败反馈页面
        versions = conn.execute(
            "SELECT * FROM published_versions ORDER BY id DESC LIMIT 20").fetchall()
        return templates.TemplateResponse(request, "publish.html", {
            "versions": versions,
            "error": f"发布过程中发生错误，已回滚且未产生新版本：{exc}",
            "month1": month1, "month2": month2,
            "non_finite_cnt": _non_finite_reading_count(conn),
        }, status_code=500)
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 版本

def _get_version_or_404(conn, version_id: int):
    version = conn.execute(
        "SELECT * FROM published_versions WHERE id = ?", (version_id,)).fetchone()
    if version is None:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="版本不存在")
    details = conn.execute(
        "SELECT * FROM version_details WHERE version_id = ?"
        " ORDER BY end_ts, period_no", (version_id,)).fetchall()
    totals = conn.execute(
        "SELECT * FROM version_monthly_totals WHERE version_id = ?"
        " ORDER BY usage_month", (version_id,)).fetchall()
    return version, details, totals


@app.get("/versions", response_class=HTMLResponse)
def versions_page(request: Request):
    conn = get_conn()
    try:
        versions = conn.execute(
            "SELECT * FROM published_versions ORDER BY id DESC").fetchall()
        current_hash = readings_state_hash(
            conn.execute("SELECT ts, period_no, cumulative FROM readings").fetchall())
        return templates.TemplateResponse(request, "versions.html", {
            "versions": versions,
            "current_hash": current_hash,
        })
    finally:
        close_conn(conn)


@app.get("/versions/{version_id}", response_class=HTMLResponse)
def version_detail_page(request: Request, version_id: int):
    conn = get_conn()
    try:
        version, details, totals = _get_version_or_404(conn, version_id)
        current_hash = readings_state_hash(
            conn.execute("SELECT ts, period_no, cumulative FROM readings").fetchall())
        return templates.TemplateResponse(request, "version_detail.html", {
            "version": version, "details": details,
            "totals": totals, "current_hash": current_hash,
        })
    finally:
        close_conn(conn)


def _csv_response(headers: list[str], rows: list[list], filename: str,
                  banner: list[str] | None = None) -> Response:
    buf = io.StringIO()
    writer = csv.writer(buf)
    # 不可装载版本的下载文件以注释行标明，避免被下游静默装载
    for line in banner or []:
        writer.writerow([line])
    writer.writerow(headers)
    writer.writerows(rows)
    # utf-8-sig BOM，保证 Excel 打开中文/表头不乱码
    return Response(
        content="﻿" + buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/versions/{version_id}/details.csv")
def download_details(version_id: int):
    conn = get_conn()
    try:
        version, details, _ = _get_version_or_404(conn, version_id)
        banner = None
        if not version["is_loadable"]:
            banner = [
                f"# WARNING version {version_id} NOT LOADABLE: contains non-finite"
                " (NaN/Infinity) usage, do not load into data warehouse",
            ]
        return _csv_response(
            ["usage_month", "start_ts_utc", "end_ts_utc", "period_no",
             "start_cumulative_kwh", "end_cumulative_kwh", "usage_kwh"],
            [[d["usage_month"], d["start_ts"], d["end_ts"], d["period_no"],
              d["start_cumulative"], d["end_cumulative"], d["usage_kwh"]]
             for d in details],
            f"version_{version_id}_details.csv", banner=banner)
    finally:
        close_conn(conn)


@app.get("/versions/{version_id}/totals.csv")
def download_totals(version_id: int):
    conn = get_conn()
    try:
        version, _, totals = _get_version_or_404(conn, version_id)
        banner = None
        if not version["is_loadable"]:
            banner = [
                f"# WARNING version {version_id} NOT LOADABLE: contains non-finite"
                " (NaN/Infinity) totals, do not load into data warehouse",
            ]
        return _csv_response(
            ["usage_month", "total_kwh"],
            [[t["usage_month"], t["total_kwh"]] for t in totals],
            f"version_{version_id}_totals.csv", banner=banner)
    finally:
        close_conn(conn)
