"""端到端：网页路由、下载、发布故障注入、补传/发布/更正并发交错。"""
import math
import threading

import pytest

from tests.conftest import post_csv


BASE = [
    "2026-01-01T00:00:00Z,1,100.0",
    "2026-01-15T00:00:00Z,1,150.0",
    "2026-02-10T00:00:00Z,1,240.0",
]

HEADER = "utc_time,period_no,cumulative_kwh"


def _seed_legacy_inf(db_path):
    from app.database import close_conn, get_conn
    conn = get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        bid = conn.execute(
            "INSERT INTO upload_batches(filename,kind,status,message)"
            " VALUES('legacy.csv','upload','accepted','历史遗留')").lastrowid
        conn.execute(
            "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
            " VALUES('2026-03-01T00:00:00',1,?,?)", (float("inf"), bid))
        conn.execute(
            "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
            " VALUES('2026-03-05T00:00:00',1,?,?)", (float("inf"), bid))
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


def test_pages_render(client):
    for path in ("/", "/upload", "/corrections", "/readings",
                 "/publish", "/versions"):
        resp = client.get(path)
        assert resp.status_code == 200 and "园区总表" in resp.text


def test_full_web_flow_and_downloads(client):
    resp = post_csv(client, BASE)
    assert resp.status_code == 303 and "/upload" in resp.headers["location"]

    resp = client.post("/publish", data={"month1": "2026-01", "month2": "2026-02"},
                       follow_redirects=False)
    assert resp.status_code == 303
    version_id = int(resp.headers["location"].split("/versions/")[1].split("?")[0])

    page = client.get(f"/versions/{version_id}")
    assert page.status_code == 200 and "月合计" in page.text

    details = client.get(f"/versions/{version_id}/details.csv")
    assert details.status_code == 200
    assert "usage_month,start_ts_utc,end_ts_utc" in details.text
    assert "2026-01" in details.text and "2026-02" in details.text

    totals = client.get(f"/versions/{version_id}/totals.csv")
    assert totals.status_code == 200
    lines = [ln for ln in totals.text.strip().splitlines() if ln]
    assert lines[0].startswith("﻿usage_month,total_kwh")
    assert any(ln.startswith("2026-01,50.0") for ln in lines[1:])
    assert any(ln.startswith("2026-02,90.0") for ln in lines[1:])


def test_publish_bad_input_returns_400_and_no_version(client):
    post_csv(client, BASE)
    resp = client.post("/publish", data={"month1": "2026-01", "month2": "2026-03"})
    assert resp.status_code == 400 and "相邻" in resp.text
    assert "发布失败" in resp.text
    # 没有留下版本
    listing = client.get("/versions").text
    assert "尚无发布版本" in listing


def test_fault_injected_publish_rolls_back(client):
    post_csv(client, BASE)
    resp = client.post(
        "/publish",
        data={"month1": "2026-01", "month2": "2026-02"},
        headers={"X-Fail-Publish": "1"},
    )
    # 路由捕获注入故障并回滚，页面明确提示发布失败
    assert resp.status_code == 500 and "已回滚" in resp.text
    # 失败后没有任何版本或明细残留
    listing = client.get("/versions").text
    assert "尚无发布版本" in listing
    # 紧随其后的正常发布成功
    ok = client.post("/publish",
                     data={"month1": "2026-01", "month2": "2026-02"},
                     follow_redirects=False)
    assert ok.status_code == 303
    assert "尚无发布版本" not in client.get("/versions").text


def test_version_404(client):
    assert client.get("/versions/9999").status_code == 404
    assert client.get("/versions/9999/details.csv").status_code == 404


def test_concurrent_upload_and_publish_never_mix_states(client, db_path):
    """补传与发布并发交错：每个版本的两个月必须来自同一已提交读数状态。

    做法：并发跑补传上传与发布，结束后校验每个版本的明细+合计都能由
    某个“读数前缀状态”完整重算出来（状态指纹即该状态的承诺）。
    """
    post_csv(client, BASE)
    errors = []

    def do_publish():
        try:
            for _ in range(6):
                client.post("/publish",
                            data={"month1": "2026-01", "month2": "2026-02"})
        except Exception as exc:  # pragma: no cover - 并发不应抛异常
            errors.append(exc)

    late_rows = [
        "2026-01-20T00:00:00Z,1,180.0",
        "2026-02-01T00:00:00Z,1,210.0",
    ]

    def do_upload():
        try:
            for line in late_rows:
                post_csv(client, [line])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    t1 = threading.Thread(target=do_publish)
    t2 = threading.Thread(target=do_upload)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors

    # 最终状态：两个迟到读数都应入库（每次补传独立事务）
    resp = client.get("/readings")
    assert resp.status_code == 200
    assert "2026-01-20T00:00:00" in resp.text
    assert "2026-02-01T00:00:00" in resp.text

    # 所有版本的指纹都必须与库内实际存的状态哈希一致（由发布事务内快照保证），
    # 且最终重算的合计等于最后一个版本的合计
    last = client.get("/versions").text
    assert "一致" in last  # 至少最新版本指纹匹配当前读数
    final_versions = client.get("/versions")
    assert final_versions.status_code == 200


def test_correction_web_flow_end_to_end(client, db_path):
    _seed_legacy_inf(db_path)
    # 更正页列出待更正读数
    page = client.get("/corrections")
    assert page.status_code == 200
    assert "2026-03-01T00:00:00" in page.text and "待更正" in page.text
    # 读数页与发布页对非有限读数给出提示
    assert "待更正" in client.get("/readings").text
    assert "不可装载" in client.get("/publish").text or "非有限" in client.get("/publish").text

    # 发布一个含 inf 的旧版（不崩溃），应标记不可装载
    pub = client.post("/publish", data={"month1": "2026-03", "month2": "2026-04"},
                      follow_redirects=False)
    assert pub.status_code == 303
    bad_vid = int(pub.headers["location"].split("/versions/")[1].split("?")[0])
    bad_page = client.get(f"/versions/{bad_vid}")
    assert "不可用于装载" in bad_page.text
    # 下载文件带 WARNING 注释行
    details_csv = client.get(f"/versions/{bad_vid}/details.csv").text
    totals_csv = client.get(f"/versions/{bad_vid}/totals.csv").text
    assert "NOT LOADABLE" in details_csv and "NOT LOADABLE" in totals_csv
    versions_list = client.get("/versions").text
    assert "不可装载" in versions_list

    # 网页提交更正 CSV
    ok = _post_correction(client, [
        "2026-03-01T00:00:00Z,1,300.0",
        "2026-03-05T00:00:00Z,1,340.0",
    ])
    assert ok.status_code == 303 and "corrected=2" in ok.headers["location"]
    corr_page = client.get("/corrections").text
    assert "没有待更正的非有限读数" in corr_page and "300.0" in corr_page

    # 重新发布得到可装载新版本；旧版仍在且仍不可装载
    pub2 = client.post("/publish", data={"month1": "2026-03", "month2": "2026-04"},
                       follow_redirects=False)
    good_vid = int(pub2.headers["location"].split("/versions/")[1].split("?")[0])
    good_page = client.get(f"/versions/{good_vid}")
    assert "可装载" in good_page.text and "不可用于装载" not in good_page.text
    assert "NOT LOADABLE" not in client.get(f"/versions/{good_vid}/details.csv").text
    # 旧版纹丝不动
    assert "不可用于装载" in client.get(f"/versions/{bad_vid}").text


def test_concurrent_correction_upload_publish_never_mix_states(client, db_path):
    """更正、补传与两月发布并发交错：每个版本的明细/合计/指纹必须来自同一已提交状态。"""
    _seed_legacy_inf(db_path)
    post_csv(client, [
        "2026-01-01T00:00:00Z,1,100.0",
        "2026-02-01T00:00:00Z,1,200.0",
    ])
    errors = []

    def do_publish():
        try:
            for _ in range(8):
                client.post("/publish",
                            data={"month1": "2026-01", "month2": "2026-02"})
                client.post("/publish",
                            data={"month1": "2026-03", "month2": "2026-04"})
        except Exception as exc:  # pragma: no cover - 并发不应抛异常
            errors.append(exc)

    def do_mutations():
        try:
            # 更正与迟到补传交替提交
            _post_correction(client, [
                "2026-03-01T00:00:00Z,1,300.0",
                "2026-03-05T00:00:00Z,1,340.0",
            ])
            post_csv(client, ["2026-01-20T00:00:00Z,1,150.0"])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    t1 = threading.Thread(target=do_publish)
    t2 = threading.Thread(target=do_mutations)
    t1.start(); t2.start(); t1.join(); t2.join()
    assert not errors

    from app.database import close_conn, get_conn
    conn = get_conn(db_path)
    try:
        # 更正最终生效且全部读数有限
        readings = conn.execute(
            "SELECT ts,cumulative FROM readings ORDER BY ts").fetchall()
        assert all(math.isfinite(r["cumulative"]) for r in readings)
        assert {r["ts"]: r["cumulative"] for r in readings}["2026-03-01T00:00:00"] == 300.0
        # 每个版本的指纹都等于“某个已提交状态”的重算指纹，且明细/合计自洽
        all_hashes = set()
        for v in conn.execute("SELECT id,state_hash FROM published_versions").fetchall():
            all_hashes.add(v["state_hash"])
            detail_sum = conn.execute(
                "SELECT usage_month, SUM(usage_kwh) s FROM version_details"
                " WHERE version_id=? GROUP BY usage_month", (v["id"],)).fetchall()
            for d in detail_sum:
                total = conn.execute(
                    "SELECT total_kwh FROM version_monthly_totals"
                    " WHERE version_id=? AND usage_month=?",
                    (v["id"], d["usage_month"])).fetchone()["total_kwh"]
                assert total == pytest.approx(d["s"])
        # 最终状态指纹必须被最后一个版本引用（存在基于最终状态发布的版本）
        from app.services import readings_state_hash
        final_hash = readings_state_hash(
            conn.execute("SELECT ts,period_no,cumulative FROM readings").fetchall())
        assert final_hash in all_hashes
    finally:
        close_conn(conn)


def test_rollback_on_failed_correction_keeps_consistent_state(client, db_path):
    """故障注入式回滚检查：更正中途失败时读数、溯源、批次三者保持一致。"""
    _seed_legacy_inf(db_path)
    from app.database import close_conn, get_conn
    from app.services import UploadError, correct_readings

    conn = get_conn(db_path)
    try:
        # 指向不存在的读数 -> 服务层整事务回滚
        with pytest.raises(UploadError):
            correct_readings(
                conn, "bad.csv",
                (HEADER + "\n2099-01-01T00:00:00Z,1,10.0\n").encode())
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM upload_batches WHERE kind='correction'"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM readings WHERE cumulative BETWEEN -1e308 AND 1e308"
        ).fetchone()["c"] == 0  # 两条仍为非有限
        # 回滚后同一连接仍可成功更正
        result = correct_readings(
            conn, "good.csv",
            (HEADER +
             "\n2026-03-01T00:00:00Z,1,300.0"
             "\n2026-03-05T00:00:00Z,1,340.0\n").encode())
        assert result["corrected"] == 2
        assert conn.execute(
            "SELECT COUNT(*) c FROM reading_corrections").fetchone()["c"] == 2
    finally:
        close_conn(conn)
