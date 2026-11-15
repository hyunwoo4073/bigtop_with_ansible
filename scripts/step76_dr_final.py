#!/usr/bin/env python3
"""Step76 D5: bounded evidence revalidation and non-secret lab DR report.

Runs only existing verify modes. It never issues configure/backup/restore/copy.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
ID_PATTERN = re.compile(r"\A[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}\Z")
BACKUP_ID_FILE = Path("artifacts/step76/latest_backup_id")
REPORT_BASENAME = "dr-final-evidence"

# Order is intentional: local backup and restore evidence first, then remote/HDFS/ES.
STAGES = (
    ("database_backups", "scripts/run_step76_backup.sh", "STEP76_LOGICAL_VERIFY_PASS", ("verify",)),
    ("scratch_restore_proofs", "scripts/run_step76_restore.sh", "STEP76_ISOLATED_VERIFY_PASS", ("verify", "both")),
    ("worker1_offhost_database_copy", "scripts/run_step76_offhost.sh", "STEP76_OFFHOST_VERIFY_PASS", ("verify",)),
    ("hdfs_single_table_proof", "scripts/run_step76_hdfs.sh", "STEP76_HDFS_VERIFY_PASS", ("verify",)),
    ("elasticsearch_snapshot_sample_restore", "scripts/run_step76_es.sh", "STEP76_ES_VERIFY_PASS", ("verify",)),
)


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def backup_id_from_file(root: Path) -> str:
    marker = root / BACKUP_ID_FILE
    if marker.is_symlink() or not marker.is_file():
        raise ValueError(f"Step76 backup ID file missing or symlinked: {marker}")
    identity = marker.read_text(encoding="utf-8").strip()
    if not ID_PATTERN.fullmatch(identity):
        raise ValueError("Invalid Step76 backup ID")
    return identity


def plan(root: Path):
    missing = [script for _, script, _, _ in STAGES if not (root / script).is_file()]
    if missing:
        raise ValueError("Required earlier Step76 verifier missing: " + ", ".join(missing))
    return STAGES


def run_stage(root: Path, stage, *, run=subprocess.run) -> dict:
    key, rel_script, marker, args = stage
    command = ["bash", rel_script, *args]
    started = time.monotonic()
    try:
        result = run(command, cwd=str(root), stdout=subprocess.PIPE,
                     stderr=subprocess.STDOUT, text=True, errors="replace",
                     timeout=900, check=False)
        output = result.stdout or ""
        ok = result.returncode == 0 and marker in output
        reason = None if ok else ("nonzero exit status" if result.returncode else "success marker missing")
    except subprocess.TimeoutExpired:
        ok = False
        reason = "verification timeout"
        output = ""
    elapsed = round(time.monotonic() - started, 2)
    record = {"stage": key, "status": "PASS" if ok else "FAIL", "marker": marker,
              "duration_seconds_verification_only": elapsed}
    if reason:
        record["failure_kind"] = reason
    # Never store full stdout; it may contain host configuration or secrets.
    return record


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, name = tempfile.mkstemp(prefix=".step76-d5-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def markdown_report(report: dict) -> str:
    statuses = "\n".join(
        f"| {s['stage']} | {s['status']} | {s['marker']} |" for s in report["stages"]
    )
    return f"""# Step76 D5 — DR 검증 증빙 통합 보고서

- 백업 ID: `{report['backup_id']}`
- 검증 시각(UTC): `{report['verified_at_utc']}`
- 검증 상태: **{report['result']}**
- 방법: 기구현된 `verify` 모드 재실행. 새 백업/복원/서비스 재시작 없음.

| 검증 단계 | 상태 | 완료 마커 |
|---|---|---|
{statuses}

## 이번 실습에서 검증한 범위

1. Iceberg REST Catalog PostgreSQL 및 OpenMetadata MySQL 논리 백업의 무결성
2. 기존 DB 서버의 격리 임시 DB 복원 결과 증빙 (새 복원을 다시 수행하지 않음)
3. worker1에 보존한 DB 백업 사본의 원본 SHA-256 대조
4. HDFS Iceberg `olist56/catalog_smoke` **단일 테이블**의 원격 파일 백업·격리 복원 증빙
5. metadata VM에 저장된 Elasticsearch Snapshot 및 **작은 인덱스 1개**의 복원·임시 인덱스 제거 증빙

## 아직 증명하지 않은 항목

- 전체 Iceberg Warehouse의 한 시점 일관성 및 전체 테이블 복구
- PostgreSQL·MySQL·Iceberg·Elasticsearch 간 동일 시점의 애플리케이션 일관성
- Elasticsearch Snapshot 저장소의 VM 외부 복제 및 노드 손실 후 실제 재구축
- 물리 호스트·스토리지 전체 장애에 대비한 독립 DR 사이트
- 전체 장애 시나리오의 RPO/RTO 측정 (이번 시간은 `verify` 실행 시간일 뿐 RTO가 아님)
- 운영 서비스의 실제 중단 후 재구축 및 복구 순서 실행

## 운영 정책

- 기존 Step65 Streaming 및 Kafka 서비스 중단/수정 없음
- `artifacts/step76/`의 리포트와 각 VM의 원본 증빙·백업은 삭제하지 않음
- 이 보고서는 **제한된 실습 범위의 DR 증빙 검증**이지, 전체 플랫폼 DR 완료 증명이 아님
- 상세 복구 순서·저장소 경로: `docs/76e-dr-final-runbook.md`
"""


def atomic_text(path: Path, content: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    fd, name = tempfile.mkstemp(prefix=".step76-d5-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.chmod(name, 0o600)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def execute(root: Path, *, runner=run_stage) -> dict:
    identity = backup_id_from_file(root)
    stages = plan(root)
    report = {
        "schema_version": 1,
        "backup_id": identity,
        "verified_at_utc": utc_now(),
        "result": "INCOMPLETE",
        "scope": "bounded_lab_evidence_only",
        "stages": [],
        "limits": ["hdfs_only_single_smoke_table", "es_snapshot_same_vm_only",
                   "no_cross_system_point_in_time_consistency", "rpo_rto_not_measured"],
    }
    for stage in stages:
        print(f"[D5] Verify: {stage[0]}", flush=True)
        entry = runner(root, stage)
        report["stages"].append(entry)
        print(f"[D5] {entry['stage']}: {entry['status']}", flush=True)
        if entry["status"] != "PASS":
            report["result"] = "FAIL"
            break
    else:
        report["result"] = "STEP76_D5_LAB_EVIDENCE_PASS"

    report_dir = root / "artifacts" / "step76"
    atomic_json(report_dir / f"{REPORT_BASENAME}-{identity}.json", report)
    atomic_text(report_dir / f"{REPORT_BASENAME}-{identity}.md", markdown_report(report))
    return report


def main() -> int:
    try:
        result = execute(ROOT)
    except (ValueError, OSError) as ex:
        print("STEP76_D5_ERROR: " + str(ex), file=sys.stderr)
        return 2
    print("[D5] Report: artifacts/step76/dr-final-evidence-" + result["backup_id"] + ".md")
    if result["result"] != "STEP76_D5_LAB_EVIDENCE_PASS":
        print("STEP76_D5_FAIL: inspect the failing stage; do not rerun backup/copy/restore", file=sys.stderr)
        return 1
    print("STEP76_D5_LAB_EVIDENCE_PASS")
    print("STEP76_D5_FULL_DR_NOT_PROVEN: Elasticsearch off-host and full-warehouse DR remain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
