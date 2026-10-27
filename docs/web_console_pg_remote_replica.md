# PostgreSQL 백업 오프노드 복제 (38단계)

## 개요

- 원본: worker3, `/var/backups/bigtop-web-console/postgresql`
- 복제 대상: worker2 (Ansible Inventory의 `worker2`), `/var/backups/bigtop-web-console-replica/postgresql`
- 백업 소스: 34번 Playbook에서 생성한 `bigtop_console_*.dump` 파일
- 매시간 복제, 백업별 `pg_restore --list` 사전 검사 및 SSH 전송 후 SHA-256 비교
- 받는 서버는 비특권 전용 계정 `bigtop_pg_replica`를 사용하고 암호 인증 불가능하게 설정
- 30일 경과 파일은 별도 타이머로 정리. **최근 파일이 없으면 과거 파일을 삭제하지 않음**
- PostgreSQL 서버 설치/복원/데이터 변경 없음. 34~37번 Playbook 파일은 건드리지 않음

### 적용 시 중요

`worker3`에서 실행한다. **`-e ansible_connection=local`을 쓰면 안 됨.**
Playbook 첫 번째와 세 번째 play는 `connection: local`, 가운데 receiver play는 Ansible SSH임.
기본 `worker2`에 대해 기존 Ansible의 SSH와 sudo가 동작해야 함.

```bash
make lint
.venv/bin/ansible-playbook playbooks/38-web-console-pg-remote-replica.yml
```

`worker2` 대신 `worker1`을 사용하려면:

```bash
.venv/bin/ansible-playbook playbooks/38-web-console-pg-remote-replica.yml \
  -e web_console_pg_remote_replica_target_host=worker1
```

한 번 다른 대상에 설치한 뒤 대상을 변경하면 기존 대상의 파일/계정/타이머는 자동 삭제하지 않는다.

### 최초 실제 복제

```bash
sudo systemctl start bigtop-web-console-pg-replica.service
sudo journalctl -u bigtop-web-console-pg-replica.service -n 40 --no-pager
systemctl list-timers --all | grep bigtop-web-console-pg-replica
```

정상 로그: `REPLICA OK target=bigtop_pg_replica@... files_verified=N`

receiver에서 보존 현황 (root로 조회):

```bash
sudo ls -lh /var/backups/bigtop-web-console-replica/postgresql/
systemctl list-timers --all | grep bigtop-web-console-pg-replica-prune
```

### 복구 원칙

worker3 VM 분실 시 worker2에서 파일을 회수해 **새 worker3의 임시 DB에 먼저 복원 검사**한다.
34번 복원 검증기의 `pg_restore_check.sh`와 같은 방식으로 테스트하며,
운영 DB를 `pg_restore --clean` 등으로 직접 덮어쓰지 않는다.

예시: 재구축 완료 후 관리자 권한으로 안전한 경로에 dump를 복사하고 소유권을 `postgres`로
맞춘 다음 `sudo -u postgres pg_restore --list /path/archive.dump`로 아카이브 목록을 확인한다.
자동 복원/운영 DB 교체 기능은 의도적으로 포함하지 않았다.

### 주의사항

- 수신 경로는 원격 서버 로컬 디스크를 사용한다. 별도 VM이더라도 같은 물리 디스크라면 공통 장애에 취약함.
- HDFS DataNode와 디스크를 공유하는 worker2에서는 용량과 I/O 부하를 별도로 감시해야 함.
- rsync 전용 SSH 키는 postgres만 읽을 수 있다. `known_hosts`는 대상의 호스트 키를
  기존 Ansible SSH 접속을 통해 확보한 뒤 고정한다.
- 전용 수신 계정은 sudo 권한이 없고, OpenSSH `restrict` 옵션으로 포트포워딩/PTY 등을 차단한다.
  다만 원격 파일 쓰기 권한을 가진 계정이므로 별도 접근제어 및 키 수명 관리가 필요하다.
- 송신 실패는 systemd 로그에 남는다. 메트릭/Alertmanager 연동은 후속 단계 대상임.
- 최초 복제는 로컬 14일치 백업을 모두 전송하므로 용량을 사전 확인한다.
