"""SQLite 连接与建表。

写操作统一使用 BEGIN IMMEDIATE，保证补传、更正与发布交错时在数据库层串行化，
并以事务保证“整批”原子性：上传/更正失败不写入任何读数，发布失败不留新版本。

历史遗留：早期版本允许 Infinity 进入 readings（NaN 会被 NOT NULL 拒绝），
两笔 Infinity 的差额为 NaN 会使发布失败。现版本：
- 普通上传遇 NaN/Infinity 整批拒收（见 services.parse_csv）；
- 已入库的非有限读数只能通过“更正批次”替换为有限值，保留原批次与更正来源；
- published_versions.is_loadable 标记旧版明细是否含非有限用量，不可装载旧版
  仍永久保留、可下载，但页面与接口明确标明不可用于装载。
"""
import math
import os
import sqlite3

from . import config


def get_conn(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or config.DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL 提升读写并发能力；单文件 SQLite 仍由写事务保证一致性
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def close_conn(conn: sqlite3.Connection) -> None:
    """关闭前 checkpoint，把 WAL 内容并入主库，保证后续连接可见已提交数据。"""
    try:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error:
        pass
    conn.close()


SCHEMA = """
-- 已提交的总表读数（累计 kWh）
CREATE TABLE IF NOT EXISTS readings (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT    NOT NULL,          -- UTC 规范时间 YYYY-MM-DDTHH:MM:SS
    period_no    INTEGER NOT NULL,          -- 表计周期号（换表即换周期）
    cumulative   REAL    NOT NULL,          -- 累计读数 kWh（现版本仅允许有限值）
    batch_id     INTEGER NOT NULL REFERENCES upload_batches(id),
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE(ts, period_no)                   -- 同刻同周期只允许一条
);

-- 上传/更正批次，接受与拒收均留痕
CREATE TABLE IF NOT EXISTS upload_batches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT    NOT NULL,
    kind         TEXT    NOT NULL DEFAULT 'upload'
                 CHECK (kind IN ('upload', 'correction')),  -- 普通上传 / 异常更正
    status       TEXT    NOT NULL CHECK (status IN ('accepted', 'rejected')),
    inserted_cnt INTEGER NOT NULL DEFAULT 0,
    skipped_cnt  INTEGER NOT NULL DEFAULT 0,
    corrected_cnt INTEGER NOT NULL DEFAULT 0,               -- 更正批次替换的读数条数
    message      TEXT    NOT NULL,
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 读数更正溯源：每条被替换的非有限读数对应一行（与读数、原批次、更正批次同时落库）
CREATE TABLE IF NOT EXISTS reading_corrections (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_id      INTEGER NOT NULL REFERENCES readings(id),
    original_batch_id INTEGER NOT NULL REFERENCES upload_batches(id), -- 原上传批次
    correction_batch_id INTEGER NOT NULL REFERENCES upload_batches(id), -- 更正来源批次
    old_cumulative  REAL    NOT NULL,         -- 替换前的非有限值（Infinity 等）
    new_cumulative  REAL    NOT NULL,         -- 替换后的有限值
    created_at      TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 一次发布生成一个版本（相邻两个 UTC 月作为一个区间）
CREATE TABLE IF NOT EXISTS published_versions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    month1       TEXT    NOT NULL,          -- 起始月 YYYY-MM
    month2       TEXT    NOT NULL,          -- 相邻的下一自然月 YYYY-MM
    state_hash   TEXT    NOT NULL,          -- 发布时全部已提交读数状态指纹
    is_loadable  INTEGER NOT NULL DEFAULT 1,-- 明细/合计是否全部有限、可用于装载（0=否）
    created_at   TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- 版本内的用量明细（相邻同周期读数差额，归入后笔所在月）
CREATE TABLE IF NOT EXISTS version_details (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES published_versions(id),
    usage_month     TEXT    NOT NULL,
    start_ts        TEXT    NOT NULL,
    end_ts          TEXT    NOT NULL,
    period_no       INTEGER NOT NULL,
    start_cumulative REAL   NOT NULL,
    end_cumulative   REAL   NOT NULL,
    usage_kwh        REAL   NOT NULL
);

-- 版本内的月合计
CREATE TABLE IF NOT EXISTS version_monthly_totals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    version_id      INTEGER NOT NULL REFERENCES published_versions(id),
    usage_month     TEXT    NOT NULL,
    total_kwh       REAL    NOT NULL,
    UNIQUE(version_id, usage_month)
);

CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings(ts);
CREATE INDEX IF NOT EXISTS idx_details_version ON version_details(version_id);
CREATE INDEX IF NOT EXISTS idx_corrections_reading ON reading_corrections(reading_id);
"""


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def _migrate(conn: sqlite3.Connection) -> None:
    """对早期建库的存量 SQLite 做幂等加列/补表，并回填版本可装载标记。"""
    batch_cols = _table_columns(conn, "upload_batches")
    if "kind" not in batch_cols:
        conn.execute(
            "ALTER TABLE upload_batches ADD COLUMN kind TEXT NOT NULL DEFAULT 'upload'")
    if "corrected_cnt" not in batch_cols:
        conn.execute(
            "ALTER TABLE upload_batches ADD COLUMN corrected_cnt INTEGER NOT NULL DEFAULT 0")

    version_cols = _table_columns(conn, "published_versions")
    add_loadable = "is_loadable" not in version_cols
    if add_loadable:
        conn.execute(
            "ALTER TABLE published_versions ADD COLUMN is_loadable INTEGER NOT NULL DEFAULT 1")

    if add_loadable:
        # 回填：明细或月合计中出现任何非有限值（Infinity/NaN 经驱动读出为 None/inf）
        # 的历史版本一律标记为不可装载；全部有限则保持可装载。
        version_ids = [
            r["version_id"] for r in conn.execute(
                "SELECT DISTINCT version_id FROM version_details").fetchall()
        ]
        for vid in version_ids:
            values = [
                r[0] for r in conn.execute(
                    "SELECT start_cumulative FROM version_details WHERE version_id=?",
                    (vid,)).fetchall()
            ]
            values += [
                r[0] for r in conn.execute(
                    "SELECT end_cumulative FROM version_details WHERE version_id=?",
                    (vid,)).fetchall()
            ]
            values += [
                r[0] for r in conn.execute(
                    "SELECT usage_kwh FROM version_details WHERE version_id=?",
                    (vid,)).fetchall()
            ]
            values += [
                r[0] for r in conn.execute(
                    "SELECT total_kwh FROM version_monthly_totals WHERE version_id=?",
                    (vid,)).fetchall()
            ]
            loadable = all(
                isinstance(v, float) and math.isfinite(v) for v in values)
            conn.execute(
                "UPDATE published_versions SET is_loadable=? WHERE id=?",
                (1 if loadable else 0, vid))


def init_db(db_path: str | None = None) -> None:
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        conn.commit()
    finally:
        close_conn(conn)
