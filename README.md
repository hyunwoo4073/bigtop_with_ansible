# Bigtop Data Platform — Ansible 기반 실습·운영 플랫폼

> **프로젝트 한눈에 보기 · 2026-10-30 기준**  
> 상세 아키텍처, 컴포넌트 선정 이유, 모든 Playbook의 역할, 배포 순서 및 운영 방법: **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)**

## 이 프로젝트는 무엇인가?

**5대 Ubuntu VM에 Hadoop 기반 데이터 처리 플랫폼과 중앙 운영 콘솔, 관측·백업 체계, OpenMetadata 데이터 거버넌스 환경을 Ansible로 구축한 개인 실습 프로젝트**다. Ambari 없이 인프라와 서비스 설정을 코드로 관리하는 것이 핵심이다.

목적은 다음 세 가지로 나뉜다.

1. **데이터 플랫폼:** HDFS에 데이터를 저장하고 YARN에서 Spark를 실행한다.
2. **운영 플랫폼:** Prometheus/Grafana/Alertmanager로 상태를 감시하고, Web Console에서 서비스를 조회·관리하며, 장애 진단과 백업·복원을 자동화한다.
3. **데이터 거버넌스:** OpenMetadata에 데이터베이스·테이블·컬럼을 수집하고 설명, 분류, 용어집, 데이터 계보를 관리한다.

## 시스템 구성

| VM | 역할 | 주요 구성 |
|---|---|---|
| `master` | 데이터 플랫폼 제어 | HDFS NameNode, YARN ResourceManager, Spark History Server |
| `worker1` | 분산 처리 | HDFS DataNode, YARN NodeManager |
| `worker2` | 분산 처리 및 백업 수신 후보 | DataNode, NodeManager, PostgreSQL 원격 백업 수신(38번 실전송 확인 필요) |
| `worker3` | 운영·자동화 중심 | Ansible Controller, Web Console(Nginx/FastAPI/React), PostgreSQL, Prometheus, Grafana, Alertmanager |
| `metadata` | 데이터 거버넌스 전용 | Docker Compose, OpenMetadata, MySQL, Elasticsearch |

- 실습 VM 자원: 기존 노드 각각 **2 vCPU/4GB**, `metadata` **4 vCPU/8GB**; 물리 PC **6코어/12스레드·32GB RAM**.
- Ansible Inventory: `[hadoop_cluster]` = master + worker1 + worker2, `[ops]` = worker3, `[governance_nodes]` = metadata. **metadata는 Hadoop 설치 대상이 아니다.**
- **NameNode·ResourceManager HA는 미구성.** 백업과 복구 절차가 HA를 대체하지는 않는다.

```text
                                 ┌───────────────────────────────────┐
                                 │ worker3: Ansible + Web Console     │
                                 │ PostgreSQL / Prometheus / Grafana  │
                                 │ Alertmanager / Ingestion CLI       │
                                 └───────┬───────────────┬───────────┘
                           SSH/Ansible   │               │ OpenMetadata API
                 ┌───────────────────────┘               ▼
                 ▼                           ┌─────────────────────────┐
 master: NameNode / RM / Spark History       │ metadata: OpenMetadata  │
       ├── worker1: DN / NM                  │ MySQL + Elasticsearch   │
       └── worker2: DN / NM                  └─────────────────────────┘
                   HDFS + YARN / Spark
```

## 컴포넌트를 넣은 이유

| 컴포넌트 | 역할과 도입 이유 |
|---|---|
| **Apache Bigtop + Ansible** | Hadoop 계열 패키지 설치와 반복 가능한 설정·배포 관리 |
| **HDFS** | 여러 노드에 데이터를 분산 저장하는 파일 시스템 |
| **YARN** | Spark 등 작업의 CPU·메모리 자원 배분 및 실행 관리 |
| **Spark / History Server** | 분산 처리와 완료된 애플리케이션 실행 이력 분석 |
| **Node Exporter + JMX Exporter** | OS·디스크 지표와 JVM/Hadoop 내부 지표 수집 |
| **Prometheus + Grafana + Alertmanager** | 메트릭 저장·평가, 시각화, 장애 알림 |
| **FastAPI + React + Nginx** | 서비스 상태·로그·작업 이력·백업 상태를 통합하는 Web Console. Nginx는 접근 인증/프록시 |
| **PostgreSQL** | Web Console Action History와 클러스터 운영 메타데이터의 영속 저장 |
| **systemd Timer + pg_dump + rsync/SSH** | 정기 백업, 무결성 검사, 원격 보관 자동화 |
| **Docker Compose + MySQL + Elasticsearch** | OpenMetadata 본체·메타데이터 DB·검색 기능을 별도 VM에서 구동 |
| **OpenMetadata + PostgreSQL Ingestion CLI** | 테이블/컬럼 카탈로그와 데이터 거버넌스 메타데이터 관리. Airflow는 현재 수집에 사용하지 않음 |

## 주요 사용자 화면

| 화면 | 접근 주소/설명 |
|---|---|
| **Bigtop Web Console** | `http://worker3:1337` — Nginx Basic Auth; 내부 FastAPI `127.0.0.1:18090` |
| NameNode | `http://master:9870` |
| YARN RM | `http://master:18088` — 콘솔 바로가기 설정값 |
| Spark History | `http://master:18080` |
| Prometheus | `http://worker3:9090` |
| Grafana | `http://worker3:23000` — 콘솔 바로가기 설정값 |
| Alertmanager | `http://worker3:19093` — 콘솔 바로가기 설정값 |
| **OpenMetadata** | `http://192.158.2.104:8585` — metadata VM, 실습 환경 주소 |

> 위 이름·주소는 실습 환경 기준이다. **바로가기 URL이 실제 데몬의 listen 포트 또는 VirtualBox 포트포워딩과 동일하다는 뜻은 아니다.** 브라우저가 `master`/`worker3`를 해석하지 못하면 Ansible 변수로 브라우저에서 접근 가능한 주소를 지정한다. `192.158.2.x`는 RFC1918 사설 IP 대역이 아니므로 네트워크 격리를 확인한다.

## 지금까지 구축한 기능

| 단계 | 범위 | 결과 |
|---|---|---|
| **01–12** | 서버 공통 설정, Java, Bigtop, HDFS/YARN/Spark 설치·기동 | 분산 저장·처리 환경 |
| **13–19** | Exporter, Prometheus/Grafana/Alertmanager, NameNode 백업·복제 | 관측 및 HDFS 메타데이터 보호 |
| **20–27** | Health/Recovery/Smoke/Diagnostics, YARN 로그, Spark History, NM 디스크 | 운영 자동화 |
| **30–33** | Web Console, PostgreSQL Action History, Ansible 운영 메타데이터, Inventory 화면 | 중앙 운영·영속 저장 |
| **34–37** | PostgreSQL 백업, 복원 검증, 백업 메트릭·알림·Admin 대시보드 | 운영 DB 백업 감시 |
| **38–39** | DB 원격 백업 복제, 서비스 Web UI 바로가기 | 38번 실제 전송 확인 필요 / 39번 적용 확인 |
| **40–42** | metadata VM Docker, OpenMetadata Core, PostgreSQL 카탈로그 수집 | 설치·수집 성공 확인 |
| **43–44** | 분류/용어집/설명 정책, SQL View 기반 Lineage PoC | 43번 실제 적용 로그 미제공 / **44번 미배포·미확인** |

> 번호 **28·29는 현재 확인한 Playbook 목록에 없다.** 44번은 패키지 제공 단계까지 진행했으며 DB View 및 OpenMetadata 계보가 실제 생성됐다는 근거는 없다.

## 시작하기 / 일상 점검

worker3의 프로젝트 루트 `~/bigtop-cluster-ansible`에서:

```bash
# Inventory 대상 확인: 5대 VM, 세 그룹 구분
.venv/bin/ansible-inventory -i inventory/hosts.ini --graph

# 원격 접근과 정적 검사
.venv/bin/ansible -i inventory/hosts.ini all -m ansible.builtin.ping
make lint
make validate

# 구축된 데이터 플랫폼 상태/복구/기능 점검
make health
make smoke
# make recover  # 서비스 복구가 필요한 경우에만 실행

# Web Console API / PostgreSQL / OpenMetadata
curl -fsS http://127.0.0.1:18090/api/health
curl -fsS http://127.0.0.1:18090/api/storage/health
curl -fsS http://192.158.2.104:8585/ -o /dev/null
```

`make validate`는 로컬 개발환경/Collection 준비 상태에 의존할 수 있다. 복구·중지·재시작 명령은 상태 조회와 다르게 서비스에 영향을 준다.

## 소스 코드 안내

```text
inventory/           호스트 그룹·환경별 설정 예제 (실제 IP/비밀정보는 Git 제외)
playbooks/           배포·재배포·점검·복구의 진입점
roles/               서비스별 Ansible 설정, 템플릿, systemd 단위
roles/web_console/   FastAPI·React/Vite·Nginx 중앙 콘솔
roles/governance_*/  OpenMetadata 관련 Docker, 수집, 정책·계보
collections/         필요한 Ansible Collection 선언
scripts/, tests/     검증·운영 스크립트·테스트
Makefile             자주 쓰는 명령을 표준화
 docs/PROJECT_GUIDE.md 전체 구조, 컴포넌트 선정 배경, Playbook별 상세 설명
```

**문서 읽는 순서:** 이 README → [상세 프로젝트 가이드](docs/PROJECT_GUIDE.md) → 필요한 영역의 `docs/` 운영 문서 및 `docs/runbooks/`.

## 운영 안전 원칙

- SSH **개인키**, JWT, DB 비밀번호, `.env`, 실제 Inventory, DB 백업 파일을 Git에 넣지 않는다.
- 원격 다중 노드 Playbook(특히 **38, 40, 41**)에는 전역 `-e ansible_connection=local`을 사용하지 않는다.
- OpenMetadata의 Docker 볼륨 삭제(`docker compose down -v`) 금지. PostgreSQL 복원 검증은 운영 DB가 아닌 임시 DB에서 수행한다.
- **Web Console의 운영 Inventory(PostgreSQL)**와 **OpenMetadata의 데이터 카탈로그(MySQL)**는 목적이 다른 시스템이다.

**문서 기준:** 이전 README 스냅샷과 31~44번 Ansible 배포 패키지, 대화 중 확인된 실제 작업 로그를 재구성했다. 현재 worker3 Git 작업 트리를 직접 읽은 결과는 아니므로 상세 가이드의 `확인 필요` 항목은 실제 리포지터리와 대조해야 한다.
