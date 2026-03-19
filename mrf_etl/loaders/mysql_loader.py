"""
mysql_loader.py
MySQL loader for mrf-etl.

Features:
  - Auto-creates all 5 tables on first run (DDL)
  - Bulk inserts (executemany) in configurable chunk sizes
  - Idempotency: skips files already loaded by source_file hash
  - Uses PyMySQL — no heavyweight ORM dependency

Install: pip install pymysql
"""

from __future__ import annotations

import hashlib
import json
from typing import Optional

try:
    import pymysql
    import pymysql.cursors
except ImportError:
    pymysql = None  # type: ignore

from mrf_etl.loaders.base_loader import BaseLoader, _mrf_row_to_item_dict, _json_safe
from mrf_etl.schema.mrf_row import HospitalMeta


# ---------------------------------------------------------------------------
# DDL — all 5 tables
# Backtick-quoted column names to avoid MySQL reserved word conflicts
# (row_number, code, description are reserved in MySQL 8.0+)
# ---------------------------------------------------------------------------

DDL_HOSPITALS = """
CREATE TABLE IF NOT EXISTS mrf_hospitals (
    `id`                    INT AUTO_INCREMENT PRIMARY KEY,
    `hospital_name`         VARCHAR(255),
    `hospital_locations`    JSON,
    `hospital_addresses`    JSON,
    `license_number`        VARCHAR(50),
    `license_state`         VARCHAR(10),
    `hospital_npi`          VARCHAR(20),
    `attester_name`         VARCHAR(255),
    `last_updated_on`       DATE,
    `as_of_date`            DATE,
    `financial_aid_policy`  TEXT,
    `cms_version`           VARCHAR(20),
    `source_file`           TEXT,
    `source_file_hash`      VARCHAR(64),
    `ingested_at`           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    `extra_metadata`        JSON,
    INDEX idx_source_hash (`source_file_hash`(64))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_ITEMS = """
CREATE TABLE IF NOT EXISTS mrf_items (
    `id`                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    `hospital_id`           INT NOT NULL,
    `description`           TEXT,
    `setting`               VARCHAR(30),
    `billing_class`         VARCHAR(30),
    `drug_unit_of_measure`  VARCHAR(50),
    `drug_type_of_measure`  VARCHAR(50),
    `gross_charge`          DECIMAL(14,4),
    `discounted_cash`       DECIMAL(14,4),
    `min_negotiated`        DECIMAL(14,4),
    `max_negotiated`        DECIMAL(14,4),
    `modifiers`             VARCHAR(200),
    `median_amount`         DECIMAL(14,4),
    `percentile_10th`       DECIMAL(14,4),
    `percentile_90th`       DECIMAL(14,4),
    `claims_count`          INT,
    `additional_notes`      TEXT,
    `footnote`              TEXT,
    `count_compared_rates`  INT,
    `extra_fields`          JSON,
    `source_file`           TEXT,
    `row_num`               INT,
    `layout_type`           VARCHAR(20),
    `ingested_at`           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hospital (`hospital_id`),
    FOREIGN KEY (`hospital_id`) REFERENCES mrf_hospitals(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_ITEM_CODES = """
CREATE TABLE IF NOT EXISTS mrf_item_codes (
    `id`                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    `item_id`               BIGINT NOT NULL,
    `code`                  VARCHAR(100),
    `code_original`         VARCHAR(100),
    `code_type`             VARCHAR(20),
    `code_index`            INT,
    `is_primary`            TINYINT(1),
    INDEX idx_item (`item_id`),
    INDEX idx_code (`code`),
    INDEX idx_code_type (`code_type`),
    FOREIGN KEY (`item_id`) REFERENCES mrf_items(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_RATES = """
CREATE TABLE IF NOT EXISTS mrf_rates (
    `id`                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    `item_id`               BIGINT NOT NULL,
    `payer_name_raw`        VARCHAR(500),
    `plan_name_raw`         VARCHAR(500),
    `plan_tier_index`       INT DEFAULT 0,
    `negotiated_dollar`     DECIMAL(14,4),
    `negotiated_percentage` DECIMAL(10,4),
    `negotiated_algorithm`  TEXT,
    `methodology`           VARCHAR(100),
    `methodology_raw`       VARCHAR(200),
    `estimated_amount`      DECIMAL(14,4),
    `rate_flag`             VARCHAR(50),
    `rate_note`             TEXT,
    `additional_notes`      TEXT,
    `setting_from_payer`    VARCHAR(30),
    `layout_source`         VARCHAR(20),
    INDEX idx_item (`item_id`),
    INDEX idx_payer (`payer_name_raw`(100)),
    FOREIGN KEY (`item_id`) REFERENCES mrf_items(`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_RAW = """
CREATE TABLE IF NOT EXISTS mrf_raw (
    `id`                    BIGINT AUTO_INCREMENT PRIMARY KEY,
    `hospital_id`           INT,
    `item_id`               BIGINT,
    `raw_row`               LONGTEXT,
    `source_file`           TEXT,
    `row_num`               INT,
    `ingested_at`           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_hospital (`hospital_id`),
    INDEX idx_item (`item_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

ALL_DDL = [DDL_HOSPITALS, DDL_ITEMS, DDL_ITEM_CODES, DDL_RATES, DDL_RAW]


# ---------------------------------------------------------------------------
# MySQLLoader
# ---------------------------------------------------------------------------

class MySQLLoader(BaseLoader):
    """
    Loads MRFRow objects into a MySQL database.

    Usage:
        loader = MySQLLoader(
            host="localhost", port=3306,
            user="root", password="pass",
            database="mrf_db"
        )
        from mrf_etl.core.pipeline import parse_file
        from mrf_etl.core.meta_parser import parse_metadata

        meta = parse_metadata("hospital.csv")
        rows = parse_file("hospital.csv")
        stats = loader.load(rows, meta, source_file="hospital.csv")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "mrf_db",
        chunk_size: int = 500,
    ):
        super().__init__(chunk_size=chunk_size)
        if pymysql is None:
            raise ImportError("pymysql is required: pip install pymysql")
        self._conn_params = dict(
            host=host, port=port, user=user,
            password=password, database=database,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )
        self._conn: Optional[pymysql.Connection] = None

    def _conn_get(self) -> pymysql.Connection:
        if self._conn is None or not self._conn.open:
            self._conn = pymysql.connect(**self._conn_params)
        return self._conn

    def close(self):
        if self._conn and self._conn.open:
            self._conn.close()

    # ── Schema ──────────────────────────────────────────────────────

    def _ensure_schema(self) -> None:
        conn = self._conn_get()
        with conn.cursor() as cur:
            for ddl in ALL_DDL:
                cur.execute(ddl)
        conn.commit()

    # ── Idempotency ─────────────────────────────────────────────────

    def file_already_loaded(self, source_file: str) -> bool:
        h = _file_hash(source_file)
        conn = self._conn_get()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT `id` FROM mrf_hospitals WHERE `source_file_hash` = %s LIMIT 1",
                (h,)
            )
            return cur.fetchone() is not None

    # ── Hospital upsert ─────────────────────────────────────────────

    def _upsert_hospital(self, meta: HospitalMeta) -> int:
        conn = self._conn_get()
        h = _file_hash(meta.source_file or "")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT `id` FROM mrf_hospitals WHERE `source_file_hash` = %s LIMIT 1",
                (h,)
            )
            existing = cur.fetchone()
            if existing:
                return existing["id"]

            cur.execute(
                """INSERT INTO mrf_hospitals
                   (`hospital_name`, `hospital_locations`, `hospital_addresses`,
                    `license_number`, `license_state`, `hospital_npi`, `attester_name`,
                    `last_updated_on`, `as_of_date`, `financial_aid_policy`,
                    `cms_version`, `source_file`, `source_file_hash`, `extra_metadata`)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    meta.hospital_name,
                    _json_safe(meta.hospital_locations),
                    _json_safe(meta.hospital_addresses),
                    meta.license_number,
                    meta.license_state,
                    meta.hospital_npi,
                    meta.attester_name,
                    meta.last_updated_on or None,
                    meta.as_of_date or None,
                    meta.financial_aid_policy,
                    meta.cms_version,
                    meta.source_file,
                    h,
                    _json_safe(meta.extra_metadata),
                )
            )
            conn.commit()
            return cur.lastrowid

    # ── Items ────────────────────────────────────────────────────────

    def _insert_items_batch(self, batch: list[dict], hospital_id: int) -> list[int]:
        conn = self._conn_get()
        sql = """
            INSERT INTO mrf_items
              (`hospital_id`, `description`, `setting`, `billing_class`,
               `drug_unit_of_measure`, `drug_type_of_measure`,
               `gross_charge`, `discounted_cash`, `min_negotiated`, `max_negotiated`,
               `modifiers`, `median_amount`, `percentile_10th`, `percentile_90th`,
               `claims_count`, `additional_notes`, `footnote`, `count_compared_rates`,
               `extra_fields`, `source_file`, `row_num`, `layout_type`)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        ids = []
        with conn.cursor() as cur:
            for item in batch:
                cur.execute(sql, (
                    hospital_id,
                    item["description"],
                    item["setting"],
                    item["billing_class"],
                    item["drug_unit_of_measure"],
                    item["drug_type_of_measure"],
                    item["gross_charge"],
                    item["discounted_cash"],
                    item["min_negotiated"],
                    item["max_negotiated"],
                    item["modifiers"],
                    item["median_amount"],
                    item["percentile_10th"],
                    item["percentile_90th"],
                    item["claims_count"],
                    item["additional_notes"],
                    item["footnote"],
                    item["count_compared_rates"],
                    item["extra_fields"],
                    item["source_file"],
                    item["row_number"],
                    item["layout_type"],
                ))
                ids.append(cur.lastrowid)
        conn.commit()
        return ids

    # ── Codes ────────────────────────────────────────────────────────

    def _insert_codes_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        conn = self._conn_get()
        sql = """
            INSERT INTO mrf_item_codes
              (`item_id`, `code`, `code_original`, `code_type`, `code_index`, `is_primary`)
            VALUES (%s,%s,%s,%s,%s,%s)
        """
        with conn.cursor() as cur:
            cur.executemany(sql, [
                (r["item_id"], r["code"], r["code_original"],
                 r["code_type"], r["code_index"], int(r["is_primary"]))
                for r in batch
            ])
        conn.commit()

    # ── Rates ────────────────────────────────────────────────────────

    def _insert_rates_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        conn = self._conn_get()
        sql = """
            INSERT INTO mrf_rates
              (`item_id`, `payer_name_raw`, `plan_name_raw`, `plan_tier_index`,
               `negotiated_dollar`, `negotiated_percentage`, `negotiated_algorithm`,
               `methodology`, `methodology_raw`, `estimated_amount`,
               `rate_flag`, `rate_note`, `additional_notes`,
               `setting_from_payer`, `layout_source`)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        with conn.cursor() as cur:
            cur.executemany(sql, [
                (r["item_id"], r["payer_name_raw"], r["plan_name_raw"],
                 r["plan_tier_index"], r["negotiated_dollar"],
                 r["negotiated_percentage"], r["negotiated_algorithm"],
                 r["methodology"], r["methodology_raw"], r["estimated_amount"],
                 r["rate_flag"], r["rate_note"], r["additional_notes"],
                 r["setting_from_payer"], r["layout_source"])
                for r in batch
            ])
        conn.commit()

    # ── Raw ──────────────────────────────────────────────────────────

    def _insert_raw_batch(self, batch: list[dict]) -> None:
        if not batch:
            return
        conn = self._conn_get()
        sql = """
            INSERT INTO mrf_raw
              (`hospital_id`, `item_id`, `raw_row`, `source_file`, `row_num`)
            VALUES (%s,%s,%s,%s,%s)
        """
        with conn.cursor() as cur:
            cur.executemany(sql, [
                (r.get("hospital_id"), r["item_id"],
                 r["raw_row"], r["source_file"], r["row_number"])
                for r in batch
            ])
        conn.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _file_hash(path: str) -> str:
    """SHA-256 hash of file path string — used as idempotency key."""
    return hashlib.sha256(path.encode()).hexdigest()