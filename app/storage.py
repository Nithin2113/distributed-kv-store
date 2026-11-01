
import hashlib
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


class KeyValueStore:
    def __init__(self, file_path="data/orbit.db"):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        self.connection = sqlite3.connect(
            str(self.file_path),
            timeout=10,
            check_same_thread=False,
        )
        self.connection.row_factory = sqlite3.Row

        self._initialize()

    def _initialize(self):
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")

        with self.connection:
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    version INTEGER NOT NULL,
                    operation_id TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0
                )
            """)

    @staticmethod
    def _checksum(value):
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def put(self, key, value, operation_id=None):
        operation_id = operation_id or str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()
        checksum = self._checksum(value)

        with self.connection:
            existing = self.connection.execute(
                "SELECT version FROM records WHERE key = ?",
                (key,),
            ).fetchone()

            version = existing["version"] + 1 if existing else 1

            self.connection.execute("""
                INSERT INTO records
                (key, value, version, operation_id, updated_at, checksum, deleted)
                VALUES (?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    version = excluded.version,
                    operation_id = excluded.operation_id,
                    updated_at = excluded.updated_at,
                    checksum = excluded.checksum,
                    deleted = 0
            """, (
                key,
                value,
                version,
                operation_id,
                timestamp,
                checksum,
            ))

        return self.get_record(key)

    def get_record(self, key):
        row = self.connection.execute(
            "SELECT * FROM records WHERE key = ?",
            (key,),
        ).fetchone()

        return dict(row) if row else None

    def get(self, key):
        record = self.get_record(key)

        if record is None or record["deleted"]:
            return None

        return record["value"]

    def delete(self, key, operation_id=None):
        record = self.get_record(key)

        if record is None or record["deleted"]:
            return None

        old_value = record["value"]
        operation_id = operation_id or str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        with self.connection:
            self.connection.execute("""
                UPDATE records
                SET value = NULL,
                    version = version + 1,
                    operation_id = ?,
                    updated_at = ?,
                    checksum = ?,
                    deleted = 1
                WHERE key = ?
            """, (
                operation_id,
                timestamp,
                self._checksum(""),
                key,
            ))

        return old_value

    def apply_record(self, record):
        required = {
            "key",
            "value",
            "version",
            "operation_id",
            "updated_at",
            "checksum",
            "deleted",
        }

        if not isinstance(record, dict) or not required.issubset(record):
            return {
                "success": False,
                "reason": "invalid_record",
            }

        key = record["key"]
        value = record["value"]
        version = record["version"]
        operation_id = record["operation_id"]
        updated_at = record["updated_at"]
        checksum = record["checksum"]
        deleted = record["deleted"]

        if not isinstance(key, str) or not key:
            return {"success": False, "reason": "invalid_key"}

        if type(version) is not int or version < 1:
            return {"success": False, "reason": "invalid_version"}

        if type(deleted) is not int or deleted not in (0, 1):
            return {"success": False, "reason": "invalid_deleted_flag"}

        if not isinstance(operation_id, str) or not operation_id:
            return {"success": False, "reason": "invalid_operation_id"}

        if not isinstance(updated_at, str):
            return {"success": False, "reason": "invalid_timestamp"}

        if deleted:
            if value is not None:
                return {"success": False, "reason": "invalid_tombstone"}
            checksum_value = ""
        else:
            if not isinstance(value, str):
                return {"success": False, "reason": "invalid_value"}
            checksum_value = value

        expected_checksum = self._checksum(checksum_value)

        if checksum != expected_checksum:
            return {"success": False, "reason": "checksum_mismatch"}

        with self.connection:
            existing = self.connection.execute(
                "SELECT * FROM records WHERE key = ?",
                (key,),
            ).fetchone()

            if existing:
                current = dict(existing)

                if version < current["version"]:
                    return {
                        "success": False,
                        "reason": "stale_version",
                    }

                if version == current["version"]:
                    same_record = (
                        operation_id == current["operation_id"]
                        and checksum == current["checksum"]
                        and deleted == current["deleted"]
                        and value == current["value"]
                    )

                    if same_record:
                        return {
                            "success": True,
                            "reason": "already_applied",
                        }

                    return {
                        "success": False,
                        "reason": "version_conflict",
                    }

            self.connection.execute("""
                INSERT INTO records
                (key, value, version, operation_id, updated_at, checksum, deleted)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    version = excluded.version,
                    operation_id = excluded.operation_id,
                    updated_at = excluded.updated_at,
                    checksum = excluded.checksum,
                    deleted = excluded.deleted
            """, (
                key,
                value,
                version,
                operation_id,
                updated_at,
                checksum,
                deleted,
            ))

        return {
            "success": True,
            "reason": "applied",
        }

    def close(self):
        self.connection.close()
