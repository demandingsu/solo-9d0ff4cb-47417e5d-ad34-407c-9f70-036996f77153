"""核心业务逻辑：CSV 解析、批次上传、异常读数更正、用量差额计算与区间发布。

规则要点
--------
1. 普通上传的累计 kWh 必须是有限数值；任何 NaN/Infinity（含 -Infinity）都
   整批拒收，绝不入库。
2. 同刻同内容重传：幂等跳过，不重复入库；同刻异内容：整批拒收。
   已经是有限值的正常读数，异值重传同样拒绝——更正流程是唯一改写通道。
3. 同周期读数倒退（后笔累计值 < 前笔）：整批拒收。
4. 仅相邻两次读数周期号相同时才计算差额，用量归入后笔所在 UTC 月；
   换表跨周期不估算。
5. 异常读数更正：更正 CSV 与普通上传同三列，且
   - 每个 (ts, period_no) 必须命中一条**已入库的非有限读数**；
   - 只允许把非有限值替换为有限值，不得新增、不得改写有限读数；
   - 替换后与全部其他读数一起复核同周期单调性；
   - 原上传批次保留，更正来源与替换前后的值记入 reading_corrections；
   - 任一更正无效或写入失败：整事务回滚，不留部分读数与更正记录。
6. 一次发布覆盖相邻两个 UTC 月，明细与月合计在同一事务中落为新版本，
   旧版本保持不变；版本绑定发布时刻的读数状态指纹，并按明细是否全部有限
   标记 is_loadable——含非有限用量的旧版保留下载但标明不可用于装载。
"""
from __future__ import annotations

import csv
import hashlib
import io
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone


class UploadError(Exception):
    """上传/更正业务错误（触发整批拒收）。"""


class PublishError(Exception):
    """发布业务错误（不产生新版本）。"""


# ---------------------------------------------------------------- 数据结构

@dataclass(frozen=True)
class ReadingRow:
    ts: str            # 规范化后的 UTC 时间字符串
    period_no: int
    cumulative: float
    line_no: int       # 原始 CSV 行号，便于报错定位


@dataclass(frozen=True)
class Interval:
    start_ts: str
    end_ts: str
    period_no: int
    start_cumulative: float
    end_cumulative: float
    usage_kwh: float
    usage_month: str   # 后笔所在月 YYYY-MM


# ---------------------------------------------------------------- CSV 解析

_HEADER_ALIASES = {
    "ts": {"ts", "utctime", "utctimestamp", "time", "timestamp",
           "datetime", "readtime", "utc时间", "时间", "时间戳"},
    "period_no": {"periodno", "period", "meterperiod", "periodnumber",
                  "cycle", "cycleno", "周期号", "表计周期号", "周期"},
    "cumulative": {"cumulative", "cumulativekwh", "kwh", "reading",
                   "total", "totalreading", "energy", "累计", "累计kwh",
                   "累计读数", "累计电量"},
}


def _normalize_header(name: str) -> str:
    return "".join(name.strip().lower().split()).replace("_", "").replace("-", "")


def _parse_ts(raw: str, line_no: int) -> str:
    raw = raw.strip()
    if not raw:
        raise UploadError(f"第 {line_no} 行：UTC 时间为空")
    text = raw.replace("Z", "+00:00") if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        raise UploadError(f"第 {line_no} 行：无法解析 UTC 时间 '{raw}'")
    if dt.tzinfo is None:
        # 必须带时区，避免把本地时间误当 UTC
        raise UploadError(f"第 {line_no} 行：时间 '{raw}' 缺少时区信息（需 UTC，如 2026-01-01T00:00:00Z）")
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S")


def _is_non_finite(value) -> bool:
    """驱动可能把非有限 REAL 读成 float('inf') 或 None（NaN 情形），统一识别。"""
    return not isinstance(value, float) or not math.isfinite(value)


def parse_csv(content: bytes) -> list[ReadingRow]:
    """解析上传的 CSV；任何格式问题（含 NaN/Infinity）都抛 UploadError（整批拒收）。"""
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise UploadError("文件不是有效的 UTF-8 编码 CSV")
    reader = csv.reader(io.StringIO(text))
    try:
        rows = list(reader)
    except csv.Error as exc:
        raise UploadError(f"CSV 解析失败：{exc}")
    if not rows:
        raise UploadError("CSV 为空，缺少表头")

    header = [_normalize_header(h) for h in rows[0]]
    col_index: dict[str, int] = {}
    for idx, name in enumerate(header):
        for field, aliases in _HEADER_ALIASES.items():
            if name in {_normalize_header(a) for a in aliases}:
                col_index[field] = idx
    missing = [f for f in ("ts", "period_no", "cumulative") if f not in col_index]
    if missing:
        raise UploadError(
            "CSV 表头缺少必需列（UTC 时间 / 表计周期号 / 累计 kWh），"
            f"实际表头：{', '.join(rows[0])}"
        )

    parsed: list[ReadingRow] = []
    seen: set[tuple[str, int]] = set()
    for offset, raw_row in enumerate(rows[1:], start=2):
        if not raw_row or all(not c.strip() for c in raw_row):
            continue  # 跳过空行
        if len(raw_row) <= max(col_index.values()):
            raise UploadError(f"第 {offset} 行：列数不足")
        ts = _parse_ts(raw_row[col_index["ts"]], offset)
        try:
            period_no = int(str(raw_row[col_index["period_no"]]).strip())
        except (ValueError, TypeError):
            raise UploadError(f"第 {offset} 行：表计周期号必须是整数")
        raw_cum = str(raw_row[col_index["cumulative"]]).strip()
        try:
            cumulative = float(raw_cum)
        except (ValueError, TypeError):
            raise UploadError(f"第 {offset} 行：累计 kWh 必须是数字")
        # NaN / Infinity（含 -Infinity）一律不允许进入任何普通上传或更正批次；
        # 更正的目的正是把库里的非有限值换成有限值。
        if not math.isfinite(cumulative):
            raise UploadError(
                f"第 {offset} 行：累计 kWh 必须是有限数值，"
                f"不接受 NaN/Infinity（收到 '{raw_cum}'），整批拒收；"
                f"已入库的异常读数请使用「异常读数更正」页面提交有限值"
            )
        key = (ts, period_no)
        if key in seen:
            raise UploadError(f"第 {offset} 行：文件内存在同刻同周期的重复读数（{ts} / 周期 {period_no}）")
        seen.add(key)
        parsed.append(ReadingRow(ts=ts, period_no=period_no,
                                 cumulative=cumulative, line_no=offset))
    if not parsed:
        raise UploadError("CSV 没有任何数据行")
    return parsed


# ---------------------------------------------------------------- 批次留痕

def _record_batch(conn: sqlite3.Connection, filename: str, status: str,
                  inserted: int, skipped: int, message: str,
                  kind: str = "upload", corrected: int = 0) -> int:
    cur = conn.execute(
        "INSERT INTO upload_batches(filename, kind, status, inserted_cnt,"
        " skipped_cnt, corrected_cnt, message)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (filename, kind, status, inserted, skipped, corrected, message),
    )
    return int(cur.lastrowid)


def _record_rejected(filename: str, message: str, kind: str = "upload") -> None:
    """拒收批次用独立事务留痕，不影响已提交读数/更正状态。"""
    from .database import close_conn, get_conn

    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _record_batch(conn, filename, "rejected", 0, 0, message, kind=kind)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 普通上传

def upload_readings(conn: sqlite3.Connection, filename: str,
                    content: bytes) -> dict:
    """整批上传：成功才提交，任何规则冲突都回滚并留拒收记录。

    含 NaN/Infinity 的 CSV 在解析阶段即被拒绝，不存在非有限值入库路径。
    """
    rows = parse_csv(content)  # 解析失败（含非有限值）直接抛错（web 层负责留痕）

    conn.execute("BEGIN IMMEDIATE")
    try:
        batch_id = _record_batch(conn, filename, "accepted", 0, 0, "处理中")

        # 已存在读数（同刻同周期唯一），一次性取出做冲突/幂等判断
        existing = {
            (r["ts"], r["period_no"]): r["cumulative"]
            for r in conn.execute("SELECT ts, period_no, cumulative FROM readings")
        }

        to_insert: list[ReadingRow] = []
        skipped = 0
        for row in rows:
            key = (row.ts, row.period_no)
            if key in existing:
                if existing[key] == row.cumulative:
                    skipped += 1  # 同刻同内容重传：幂等跳过
                else:
                    # 正常读数异值重传仍拒绝；非有限旧值也不在此通道处理，
                    # 必须走更正流程，避免普通补传悄悄改写历史读数。
                    hint = ""
                    if _is_non_finite(existing[key]):
                        hint = "；该读数为非有限异常值，请使用「异常读数更正」提交有限值"
                    raise UploadError(
                        f"第 {row.line_no} 行：{row.ts}（周期 {row.period_no}）"
                        f"已存在不同累计值 {existing[key]}，与本次 {row.cumulative} 冲突，"
                        f"整批拒收{hint}"
                    )
            else:
                to_insert.append(row)
                existing[key] = row.cumulative

        _check_monotonic(conn, existing, extra_rows=to_insert)

        for row in to_insert:
            conn.execute(
                "INSERT INTO readings(ts, period_no, cumulative, batch_id)"
                " VALUES (?, ?, ?, ?)",
                (row.ts, row.period_no, row.cumulative, batch_id),
            )

        inserted = len(to_insert)
        conn.execute(
            "UPDATE upload_batches SET inserted_cnt = ?, skipped_cnt = ?,"
            " message = ? WHERE id = ?",
            (inserted, skipped,
             f"接受 {inserted} 条，跳过重复 {skipped} 条", batch_id),
        )
        conn.commit()
        return {"batch_id": batch_id, "inserted": inserted,
                "skipped": skipped, "status": "accepted"}
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------- 异常读数更正

def correct_readings(conn: sqlite3.Connection, filename: str,
                     content: bytes) -> dict:
    """用同三列更正 CSV 把已入库的非有限读数替换为有限值。

    严格约束：
    - CSV 自身必须全部为有限值（parse_csv 已拒绝 NaN/Infinity）；
    - 每行必须命中一条现存读数，且该现存读数必须是非有限值；
      命中有限读数（正常读数异值重传）或查无此读数都整批拒收；
    - 全部替换与其他读数合并后复核同周期单调性，倒退则整批拒收；
    - readings 原行原地更新（保留 id、原 batch_id 与 created_at），
      更正来源写入 reading_corrections；原上传批次记录保持不变；
    - 整个流程一个事务，任何失败都回滚，不留部分读数或更正记录。
    """
    rows = parse_csv(content)

    conn.execute("BEGIN IMMEDIATE")
    try:
        batch_id = _record_batch(
            conn, filename, "accepted", 0, 0, "更正处理中", kind="correction")

        stored = {
            (r["ts"], r["period_no"]): dict(r)
            for r in conn.execute(
                "SELECT id, ts, period_no, cumulative, batch_id FROM readings")
        }

        replacements: list[dict] = []
        seen: set[tuple[str, int]] = set()
        for row in rows:
            key = (row.ts, row.period_no)
            if key in seen:
                raise UploadError(
                    f"第 {row.line_no} 行：更正文件内 {row.ts}（周期 {row.period_no}）"
                    f"重复出现，整批拒收")
            seen.add(key)
            target = stored.get(key)
            if target is None:
                raise UploadError(
                    f"第 {row.line_no} 行：{row.ts}（周期 {row.period_no}）"
                    f"在库内不存在对应读数；更正只能替换已入库的异常读数，不能新增，整批拒收")
            if not _is_non_finite(target["cumulative"]):
                raise UploadError(
                    f"第 {row.line_no} 行：{row.ts}（周期 {row.period_no}）"
                    f"已是有限值 {target['cumulative']}，正常读数不允许异值重传，整批拒收")
            replacements.append({
                "reading_id": target["id"],
                "original_batch_id": target["batch_id"],
                "old_cumulative": target["cumulative"],
                "new_cumulative": row.cumulative,
                "line_no": row.line_no,
            })

        # 以替换后的有限值构造候选状态，与库内其他读数一起做周期单调性复核
        candidate = {
            (r["ts"], r["period_no"]): r["cumulative"]
            for r in conn.execute(
                "SELECT ts, period_no, cumulative FROM readings")
        }
        for rep, row in zip(replacements, rows):
            candidate[(row.ts, row.period_no)] = row.cumulative
        _check_monotonic(conn, candidate)

        for rep, row in zip(replacements, rows):
            # 原地更新：读数 id、原上传批次 batch_id、created_at 全部保留
            conn.execute(
                "UPDATE readings SET cumulative=? WHERE id=?",
                (row.cumulative, rep["reading_id"]))
            conn.execute(
                "INSERT INTO reading_corrections(reading_id, original_batch_id,"
                " correction_batch_id, old_cumulative, new_cumulative)"
                " VALUES (?, ?, ?, ?, ?)",
                (rep["reading_id"], rep["original_batch_id"], batch_id,
                 rep["old_cumulative"], row.cumulative))

        corrected = len(replacements)
        conn.execute(
            "UPDATE upload_batches SET corrected_cnt=?, message=? WHERE id=?",
            (corrected, f"更正接受：替换 {corrected} 条非有限读数为有限值", batch_id))
        conn.commit()
        return {"batch_id": batch_id, "corrected": corrected,
                "inserted": 0, "skipped": 0, "status": "accepted"}
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------- 单调性校验

def _check_monotonic(conn: sqlite3.Connection,
                     points_map: dict[tuple[str, int], float],
                     extra_rows: list[ReadingRow] | None = None) -> None:
    """同周期按时间排序逐对检查累计值不倒退（持平允许）。

    points_map 为 (ts, period_no) -> cumulative 的候选状态；
    extra_rows 仅用于错误信息定位，校验本身基于完整候选状态，
    因此更正替换后与全部其他读数的单调性都能被复核到。
    """
    by_period: dict[int, list[tuple[str, float]]] = {}
    for (ts, period_no), cum in points_map.items():
        by_period.setdefault(period_no, []).append((ts, cum))

    line_hint = {
        (r.ts, r.period_no): r.line_no for r in (extra_rows or [])
    }
    for period_no, points in by_period.items():
        points.sort()
        for (prev_ts, prev_cum), (cur_ts, cur_cum) in zip(points, points[1:]):
            # 只在两侧均为有限值时比较大小；非有限值（历史遗留 Infinity）
            # 不参与倒退判定，待其被更正为有限值后再纳入单调性复核。
            finite_pair = (
                isinstance(prev_cum, float) and isinstance(cur_cum, float)
                and math.isfinite(prev_cum) and math.isfinite(cur_cum)
            )
            if finite_pair and cur_cum < prev_cum:
                where = line_hint.get((cur_ts, period_no))
                loc = f"第 {where} 行触发：" if where else ""
                raise UploadError(
                    f"{loc}周期 {period_no} 读数倒退：{prev_ts} 的 {prev_cum} "
                    f"-> {cur_ts} 的 {cur_cum}，整批拒收")


# ---------------------------------------------------------------- 差额计算

def _normalize_points(readings) -> list[tuple[str, int, float]]:
    """把 sqlite3.Row / dict / tuple 统一成可哈希的 (ts, period_no, cumulative)。"""
    points = []
    for r in readings:
        if isinstance(r, sqlite3.Row) or hasattr(r, "keys"):
            points.append((r["ts"], r["period_no"], r["cumulative"]))
        elif isinstance(r, dict):
            points.append((r["ts"], r["period_no"], r["cumulative"]))
        else:
            points.append((r[0], r[1], r[2]))
    return points


def compute_intervals(readings: list[sqlite3.Row | tuple]) -> list[Interval]:
    """按时间排序，仅相邻且同周期的读数计算差额；换表跨周期不估算。"""
    points = sorted(_normalize_points(readings), key=lambda x: x[0])
    intervals: list[Interval] = []
    for prev, cur in zip(points, points[1:]):
        prev_ts, prev_period, prev_cum = prev
        cur_ts, cur_period, cur_cum = cur
        if prev_period != cur_period:
            continue  # 换表跨周期：不估算
        intervals.append(Interval(
            start_ts=prev_ts,
            end_ts=cur_ts,
            period_no=cur_period,
            start_cumulative=prev_cum,
            end_cumulative=cur_cum,
            usage_kwh=cur_cum - prev_cum,
            usage_month=cur_ts[:7],
        ))
    return intervals


def readings_state_hash(readings: list[sqlite3.Row | tuple]) -> str:
    """对全部已提交读数计算规范指纹，用于证明版本基于哪个读数状态。"""
    points = sorted(_normalize_points(readings), key=lambda x: (x[0], x[1]))
    digest = hashlib.sha256()
    for ts, period_no, cum in points:
        digest.update(f"{ts}|{period_no}|{cum}\n".encode("utf-8"))
    return digest.hexdigest()


# ---------------------------------------------------------------- 区间发布

def _validate_month(value: str, label: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m")
    except (ValueError, TypeError):
        raise PublishError(f"{label} 必须是 YYYY-MM 格式的 UTC 月份")
    return value


def _all_finite(*values) -> bool:
    return all(
        isinstance(v, float) and math.isfinite(v) for v in values
    )


def _storable(value: float) -> float:
    """把计算结果落库前的非有限值规范化为可存储的 REAL。

    Python sqlite3 绑定 NaN 时会写为 NULL，撞 NOT NULL 约束——这正是早期
    “两笔 Infinity 读数使月度发布失败”的直接原因。这里把 NaN 统一映射为
    Infinity（REAL），Infinity 原样保留；版本同时被标记为不可装载，
    下游不会误用，且明细/合计行结构完整、可下载核对。
    """
    if isinstance(value, float):
        if math.isnan(value):
            return float("inf")
        return value
    return float("inf")


def publish_two_months(conn: sqlite3.Connection, month1: str, month2: str,
                       fail_before_commit: bool = False) -> dict:
    """发布相邻两个 UTC 月的区间明细与月合计，整体一个事务、一个新版本。

    读数快照、状态指纹、明细、月合计、可装载标记全部在同一 BEGIN IMMEDIATE
    事务内产生，因此更正/补传与两月发布交错时，同一版本的明细、合计与指纹
    必然来自同一个已提交读数状态，不会各取一半。
    """
    month1 = _validate_month(month1, "起始月")
    month2 = _validate_month(month2, "结束月")
    y1, m1 = (int(x) for x in month1.split("-"))
    y2, m2 = (int(x) for x in month2.split("-"))
    expected_m = m1 + 1
    expected_y = y1
    if expected_m == 13:
        expected_m, expected_y = 1, y1 + 1
    if (y2, m2) != (expected_y, expected_m):
        raise PublishError(f"{month1} 与 {month2} 不是相邻的两个 UTC 月")

    conn.execute("BEGIN IMMEDIATE")
    try:
        readings = list(conn.execute(
            "SELECT ts, period_no, cumulative FROM readings ORDER BY ts"))
        state_hash = readings_state_hash(readings)
        intervals = [
            iv for iv in compute_intervals(readings)
            if iv.usage_month in (month1, month2)
        ]

        # 可装载标记：起止累计、差额与月合计全部有限才可用于装载。
        # 历史遗留的 Infinity 读数不再导致发布崩溃，而是生成一个明确标记
        # “不可用于装载”的版本；更正为有限值后重新发布即得到可装载新版本。
        loadable = True
        for iv in intervals:
            if not _all_finite(iv.start_cumulative, iv.end_cumulative,
                               iv.usage_kwh):
                loadable = False
                break

        cur = conn.execute(
            "INSERT INTO published_versions(month1, month2, state_hash, is_loadable)"
            " VALUES (?, ?, ?, ?)",
            (month1, month2, state_hash, 1 if loadable else 0),
        )
        version_id = int(cur.lastrowid)

        for iv in intervals:
            conn.execute(
                "INSERT INTO version_details(version_id, usage_month, start_ts,"
                " end_ts, period_no, start_cumulative, end_cumulative, usage_kwh)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (version_id, iv.usage_month, iv.start_ts, iv.end_ts,
                 iv.period_no, _storable(iv.start_cumulative),
                 _storable(iv.end_cumulative), _storable(iv.usage_kwh)),
            )

        totals: dict[str, float] = {month1: 0.0, month2: 0.0}
        for iv in intervals:
            totals[iv.usage_month] += iv.usage_kwh
        for month, total in totals.items():
            if not (isinstance(total, float) and math.isfinite(total)):
                loadable = False
            conn.execute(
                "INSERT INTO version_monthly_totals(version_id, usage_month, total_kwh)"
                " VALUES (?, ?, ?)",
                (version_id, month, _storable(total)),
            )
        if not loadable:
            conn.execute(
                "UPDATE published_versions SET is_loadable=0 WHERE id=?",
                (version_id,))

        if fail_before_commit:
            # 自测用故障注入：验证回滚后不留任何新版本痕迹
            raise RuntimeError("故障注入：模拟发布提交前失败")

        conn.commit()
        return {"version_id": version_id, "month1": month1, "month2": month2,
                "state_hash": state_hash, "is_loadable": loadable,
                "detail_count": len(intervals), "totals": totals}
    except Exception:
        conn.rollback()
        raise
