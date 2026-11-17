"""异常读数恢复：非有限值拒收、更正流程、溯源、不可装载版本与原子性。"""
import math

import pytest

from app.database import close_conn, get_conn
from app.services import (
    UploadError,
    compute_intervals,
    correct_readings,
    publish_two_months,
    readings_state_hash,
    upload_readings,
)
from tests.conftest import post_csv

HEADER = "utc_time,period_no,cumulative_kwh"


def _insert_legacy_non_finite(db_path, points):
    """模拟历史遗留：直接把 Infinity 读数写入 readings（绕过现版本解析校验）。"""
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        bid = conn.execute(
            "INSERT INTO upload_batches(filename,kind,status,message)"
            " VALUES('legacy_gateway.csv','upload','accepted','历史遗留批次')"
        ).lastrowid
        for ts, period, cum in points:
            conn.execute(
                "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
                " VALUES(?,?,?,?)", (ts, period, cum, bid))
        conn.commit()
        return bid
    finally:
        close_conn(conn)


def _post_correction(client, lines, filename="correction.csv"):
    content = ("\n".join([HEADER] + lines)).encode("utf-8")
    return client.post(
        "/corrections",
        files={"file": (filename, content, "text/csv")},
        follow_redirects=False,
    )


# ------------------------------------------------------------ 普通上传拒收

@pytest.mark.parametrize("token", ["Infinity", "-Infinity", "inf", "-inf", "nan", "NaN"])
def test_normal_upload_with_non_finite_rejected(client, token):
    resp = post_csv(client, [f"2026-01-01T00:00:00Z,1,{token}",
                             "2026-01-15T00:00:00Z,1,150.0"])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM readings").fetchone()["c"] == 0
        rejected = conn.execute(
            "SELECT * FROM upload_batches WHERE status='rejected'").fetchall()
        assert len(rejected) == 1 and "有限" in rejected[0]["message"]
    finally:
        close_conn(conn)


def test_one_non_finite_in_batch_rejects_everything(client):
    # 同一批里即便只有一条 Infinity，正常的那条也不允许落地
    resp = post_csv(client, ["2026-01-01T00:00:00Z,1,100.0",
                             "2026-01-15T00:00:00Z,1,Infinity"])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn()
    try:
        assert conn.execute("SELECT COUNT(*) c FROM readings").fetchone()["c"] == 0
    finally:
        close_conn(conn)


def test_service_level_non_finite_leaves_no_batch_trace(db_path):
    conn = get_conn(db_path)
    try:
        with pytest.raises(UploadError):
            upload_readings(
                conn, "x.csv",
                (HEADER + "\n2026-01-01T00:00:00Z,1,Infinity\n").encode())
        # 服务层失败：事务回滚，连“处理中”批次头都不留，连接仍可复用
        assert conn.execute("SELECT COUNT(*) c FROM readings").fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM upload_batches").fetchone()["c"] == 0
    finally:
        close_conn(conn)


# ------------------------------------------------------------ 历史遗留 + 发布

def test_legacy_two_infinity_no_longer_crashes_publish_but_not_loadable(client, db_path):
    """复现原始故障：两笔 Infinity 曾使发布在 NOT NULL 上整体失败。"""
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf")),
        ("2026-02-01T00:00:00", 1, float("inf")),
    ])
    conn = get_conn(db_path)
    try:
        result = publish_two_months(conn, "2026-01", "2026-02")
        assert result["is_loadable"] is False
        version = conn.execute(
            "SELECT * FROM published_versions WHERE id=?",
            (result["version_id"],)).fetchone()
        assert version["is_loadable"] == 0
        # 明细行结构完整（NaN 差额被规范化为可存储 REAL，不再触发 NOT NULL）
        details = conn.execute(
            "SELECT * FROM version_details WHERE version_id=?",
            (result["version_id"],)).fetchall()
        assert len(details) == 1
        totals = conn.execute(
            "SELECT * FROM version_monthly_totals WHERE version_id=?",
            (result["version_id"],)).fetchall()
        assert len(totals) == 2
    finally:
        close_conn(conn)


def test_republish_after_correction_is_loadable_and_old_version_kept(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf")),
        ("2026-02-01T00:00:00", 1, float("inf")),
    ])
    conn = get_conn(db_path)
    bad_version = publish_two_months(conn, "2026-01", "2026-02")
    close_conn(conn)

    resp = _post_correction(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,160.0",
    ])
    assert resp.status_code == 303 and "corrected=2" in resp.headers["location"]

    conn = get_conn(db_path)
    try:
        good = publish_two_months(conn, "2026-01", "2026-02")
        assert good["is_loadable"] is True
        assert good["totals"]["2026-02"] == pytest.approx(60.0)
        # 旧版保留、指纹不同，且仍标记不可装载
        rows = conn.execute(
            "SELECT id,is_loadable,state_hash FROM published_versions ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["id"] == bad_version["version_id"] and rows[0]["is_loadable"] == 0
        assert rows[1]["id"] == good["version_id"] and rows[1]["is_loadable"] == 1
        assert rows[0]["state_hash"] != rows[1]["state_hash"]
    finally:
        close_conn(conn)


# ------------------------------------------------------------ 更正通道约束

def test_normal_upload_cannot_overwrite_non_finite_reading(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    # 同刻重传有限值走普通上传：仍拒绝（只有更正通道能改）
    resp = post_csv(client, ["2026-01-01T00:00:00Z,1,100.0"])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        row = conn.execute("SELECT cumulative FROM readings").fetchone()
        assert not math.isfinite(row["cumulative"])
    finally:
        close_conn(conn)


def test_correction_replaces_only_non_finite_and_keeps_provenance(client, db_path):
    legacy_batch = _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf")),
        ("2026-02-01T00:00:00", 1, float("-inf")),
    ])
    resp = _post_correction(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,160.0",
    ])
    assert resp.status_code == 303
    conn = get_conn(db_path)
    try:
        readings = conn.execute(
            "SELECT id,ts,period_no,cumulative,batch_id FROM readings ORDER BY ts").fetchall()
        assert [r["cumulative"] for r in readings] == [100.0, 160.0]
        # 原上传批次保留：读数仍指向原批次，行 id 不变
        assert all(r["batch_id"] == legacy_batch for r in readings)
        corrections = conn.execute(
            "SELECT * FROM reading_corrections ORDER BY id").fetchall()
        assert len(corrections) == 2
        for c in corrections:
            assert c["original_batch_id"] == legacy_batch
            assert not math.isfinite(c["old_cumulative"])
            assert math.isfinite(c["new_cumulative"])
        corr_batch = conn.execute(
            "SELECT * FROM upload_batches WHERE kind='correction' AND status='accepted'"
        ).fetchone()
        assert corr_batch["corrected_cnt"] == 2
        assert all(c["correction_batch_id"] == corr_batch["id"] for c in corrections)
    finally:
        close_conn(conn)


def test_correction_targeting_finite_reading_rejected(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    post_csv(client, ["2026-02-01T00:00:00Z,1,50.0"])
    # 第一条合法、第二条指向有限读数 -> 整批拒收，第一条也不得落地
    resp = _post_correction(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,999.0",
    ])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
        # inf 原样保留，有限读数未被改写
        rows = {r["ts"]: r["cumulative"] for r in
                conn.execute("SELECT ts,cumulative FROM readings")}
        assert not math.isfinite(rows["2026-01-01T00:00:00"])
        assert rows["2026-02-01T00:00:00"] == 50.0
    finally:
        close_conn(conn)


def test_correction_unknown_reading_rejected(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    resp = _post_correction(client, ["2027-01-01T00:00:00Z,1,10.0"])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM readings").fetchone()["c"] == 1
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
    finally:
        close_conn(conn)


def test_correction_with_non_finite_value_rejected(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    resp = _post_correction(client, ["2026-01-01T00:00:00Z,1,Infinity"])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
        assert not math.isfinite(
            conn.execute("SELECT cumulative FROM readings").fetchone()["cumulative"])
    finally:
        close_conn(conn)


def test_correction_checked_for_monotonicity_with_other_readings(client, db_path):
    # inf 两侧都是有限读数；更正为低于前笔的值会造成倒退 -> 整批拒收
    _insert_legacy_non_finite(db_path, [
        ("2026-01-15T00:00:00", 1, float("inf"))])
    post_csv(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,200.0",
    ])
    resp = _post_correction(client, ["2026-01-15T00:00:00Z,1,90.0"])  # < 前笔 100
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        assert not math.isfinite(
            conn.execute(
                "SELECT cumulative FROM readings WHERE ts='2026-01-15T00:00:00'"
            ).fetchone()["cumulative"])
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
    finally:
        close_conn(conn)

    # 合法更正值（>=100 且 <=200）接受，且差额预览恢复正常
    ok = _post_correction(client, ["2026-01-15T00:00:00Z,1,150.0"])
    assert ok.status_code == 303
    conn = get_conn(db_path)
    try:
        intervals = compute_intervals(
            conn.execute("SELECT ts,period_no,cumulative FROM readings").fetchall())
        assert all(math.isfinite(iv.usage_kwh) for iv in intervals)
        assert sum(iv.usage_kwh for iv in intervals) == pytest.approx(100.0)
    finally:
        close_conn(conn)


def test_duplicate_key_inside_correction_file_rejected(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    resp = _post_correction(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-01-01T00:00:00Z,1,120.0",
    ])
    assert resp.status_code == 303 and "UploadError" in resp.headers["location"]
    conn = get_conn(db_path)
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
    finally:
        close_conn(conn)


def test_failed_service_correction_leaves_no_records(db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    conn = get_conn(db_path)
    try:
        with pytest.raises(UploadError):
            correct_readings(
                conn, "bad.csv",
                (HEADER + "\n2027-01-01T00:00:00Z,1,10.0\n").encode())
        # 服务层整事务回滚：无更正记录、无更正批次头、读数不变
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM upload_batches WHERE kind='correction'"
        ).fetchone()["c"] == 0
        assert not math.isfinite(
            conn.execute("SELECT cumulative FROM readings").fetchone()["cumulative"])
    finally:
        close_conn(conn)


def test_web_rejected_correction_records_audit_batch_only(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf"))])
    resp = _post_correction(client, ["2027-01-01T00:00:00Z,1,10.0"])
    assert resp.status_code == 303
    conn = get_conn(db_path)
    try:
        # 网页层拒收：留一条拒收批次痕跡，但无读数变更、无更正溯源
        rejected = conn.execute(
            "SELECT * FROM upload_batches WHERE kind='correction' AND status='rejected'"
        ).fetchall()
        assert len(rejected) == 1 and rejected[0]["corrected_cnt"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
    finally:
        close_conn(conn)


# ------------------------------------------------------------ 与发布交错的一致性

def test_state_hash_changes_after_correction_and_pins_versions(client, db_path):
    _insert_legacy_non_finite(db_path, [
        ("2026-01-01T00:00:00", 1, float("inf")),
        ("2026-02-01T00:00:00", 1, float("inf")),
    ])
    conn = get_conn(db_path)
    before_hash = readings_state_hash(
        conn.execute("SELECT ts,period_no,cumulative FROM readings").fetchall())
    v1 = publish_two_months(conn, "2026-01", "2026-02")
    close_conn(conn)

    _post_correction(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,160.0",
    ])

    conn = get_conn(db_path)
    try:
        after_hash = readings_state_hash(
            conn.execute("SELECT ts,period_no,cumulative FROM readings").fetchall())
        assert after_hash != before_hash
        stored = conn.execute(
            "SELECT state_hash FROM published_versions WHERE id=?",
            (v1["version_id"],)).fetchone()["state_hash"]
        assert stored == before_hash  # 旧版指纹锁定旧状态，未被更正改写
    finally:
        close_conn(conn)


def test_migration_backfills_loadable_flag(db_path):
    """模拟旧版库（无 is_loadable 列、无更正表）的启动迁移与标记回填。"""
    import os
    import sqlite3

    from app.database import close_conn as _close
    from app.database import get_conn as _get
    from app.database import init_db

    # 夹具已按新 schema 初始化过：删掉文件，用裸连接完全按旧形态重建
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)

    raw = sqlite3.connect(db_path)
    raw.executescript(
        """
        CREATE TABLE upload_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT, filename TEXT NOT NULL,
            status TEXT NOT NULL, inserted_cnt INTEGER NOT NULL DEFAULT 0,
            skipped_cnt INTEGER NOT NULL DEFAULT 0, message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
        CREATE TABLE readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL,
            period_no INTEGER NOT NULL, cumulative REAL NOT NULL,
            batch_id INTEGER NOT NULL REFERENCES upload_batches(id),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            UNIQUE(ts, period_no));
        CREATE TABLE published_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, month1 TEXT NOT NULL,
            month2 TEXT NOT NULL, state_hash TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')));
        CREATE TABLE version_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT, version_id INTEGER NOT NULL,
            usage_month TEXT NOT NULL, start_ts TEXT NOT NULL, end_ts TEXT NOT NULL,
            period_no INTEGER NOT NULL, start_cumulative REAL NOT NULL,
            end_cumulative REAL NOT NULL, usage_kwh REAL NOT NULL);
        CREATE TABLE version_monthly_totals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, version_id INTEGER NOT NULL,
            usage_month TEXT NOT NULL, total_kwh REAL NOT NULL,
            UNIQUE(version_id, usage_month));
        """
    )
    raw.execute(
        "INSERT INTO upload_batches(filename,status,message) VALUES('legacy.csv','accepted','x')")
    bid = raw.execute("SELECT last_insert_rowid()").fetchone()[0]
    raw.execute(
        "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
        " VALUES('2026-01-01T00:00:00',1,?,?)", (float("inf"), bid))
    raw.execute(
        "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
        " VALUES('2026-02-01T00:00:00',1,?,?)", (float("inf"), bid))
    raw.execute(
        "INSERT INTO published_versions(month1,month2,state_hash)"
        " VALUES('2026-01','2026-02','oldhash')")
    vid = raw.execute("SELECT last_insert_rowid()").fetchone()[0]
    raw.execute(
        "INSERT INTO version_details(version_id,usage_month,start_ts,end_ts,"
        "period_no,start_cumulative,end_cumulative,usage_kwh)"
        " VALUES(?,?,?,?,?,?,?,?)",
        (vid, "2026-02", "2026-01-01T00:00:00", "2026-02-01T00:00:00",
         1, float("inf"), float("inf"), float("inf")))
    raw.execute(
        "INSERT INTO version_monthly_totals(version_id,usage_month,total_kwh)"
        " VALUES(?,?,?)", (vid, "2026-01", 0.0))
    raw.execute(
        "INSERT INTO version_monthly_totals(version_id,usage_month,total_kwh)"
        " VALUES(?,?,?)", (vid, "2026-02", float("inf")))
    # 再放一个全有限的旧版本，迁移后应保持可装载
    raw.execute(
        "INSERT INTO published_versions(month1,month2,state_hash)"
        " VALUES('2026-03','2026-04','finitehash')")
    vid2 = raw.execute("SELECT last_insert_rowid()").fetchone()[0]
    raw.execute(
        "INSERT INTO version_details(version_id,usage_month,start_ts,end_ts,"
        "period_no,start_cumulative,end_cumulative,usage_kwh)"
        " VALUES(?,?,?,?,?,?,?,?)",
        (vid2, "2026-03", "2026-03-01T00:00:00", "2026-03-02T00:00:00",
         1, 10.0, 12.0, 2.0))
    raw.execute(
        "INSERT INTO version_monthly_totals(version_id,usage_month,total_kwh)"
        " VALUES(?,?,?)", (vid2, "2026-03", 2.0))
    raw.execute(
        "INSERT INTO version_monthly_totals(version_id,usage_month,total_kwh)"
        " VALUES(?,?,?)", (vid2, "2026-04", 0.0))
    raw.commit()
    raw.close()

    # 启动迁移：加列、补更正表，并按内容回填可装载标记
    init_db(db_path)
    conn = _get(db_path)
    try:
        flags = {
            r["id"]: r["is_loadable"]
            for r in conn.execute("SELECT id,is_loadable FROM published_versions")}
        assert flags[vid] == 0   # 含 Infinity 的旧版 -> 不可装载
        assert flags[vid2] == 1  # 全有限旧版 -> 保持可装载
        # 新表与新列存在，旧读数/版本内容未被迁移破坏
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
        assert conn.execute(
            "SELECT kind FROM upload_batches WHERE id=?", (bid,)).fetchone()["kind"] == "upload"
        assert conn.execute(
            "SELECT COUNT(*) c FROM readings").fetchone()["c"] == 2
    finally:
        _close(conn)
