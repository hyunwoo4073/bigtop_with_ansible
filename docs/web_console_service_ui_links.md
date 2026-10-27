# Web Console 서비스 UI 바로가기 (39)

Home의 **Service Web UIs**와 Services의 **Open UI**는 실제 웹 관리 화면이
있는 서비스만 표시합니다. DataNode/NodeManager(worker1, worker2)는 기본 목록에서
제외하며, 실제 UI 서버는 master 또는 별도 모니터링 서버인 worker3입니다.

| 서비스 | 웹 UI URL(기본값) |
| --- | --- |
| HDFS NameNode | `http://master:9870` |
| YARN ResourceManager | `http://master:18088` |
| Spark History Server | `http://master:18080` |
| Prometheus | `http://worker3:9090` |
| Grafana | `http://worker3:23000` |
| Alertmanager | `http://worker3:19093` |

## 설정 반영

```bash
.venv/bin/ansible-playbook playbooks/39-web-console-service-ui-links.yml \
  -e ansible_connection=local
curl -fsS http://127.0.0.1:18090/api/service-ui-links | python3 -m json.tool
```

설정 소스는 `roles/web_console_service_ui_links/defaults/main.yml`입니다.
`inventory/group_vars/all.yml` 등에 **`web_console_service_ui_links_items`를 별도 정의했다면**
기본값보다 해당 정의가 우선합니다. 이전의 10개 목록을 지정해 둔 경우 여기서도
DataNode/NodeManager 4개 항목을 삭제하고 요청한 포트를 수정하세요.

`/etc/bigtop-web-console/service-ui-links.json`은 Ansible이 생성하는 런타임
파일입니다. **직접 수정하지 말고 Ansible 변수를 변경하세요.**

브라우저가 `master`/`worker3` 이름을 해석하지 못하는 환경에서는
브라우저에서 접근 가능한 호스트 IP 또는 포트포워딩 주소로 URL을 조정해야 합니다.
이 패키지는 HTTP(S) 링크만 제공하며 실제 서비스 listen 포트, SSH 터널,
VirtualBox NAT 포트포워딩, 인증을 변경하지 않습니다.

API는 `GET /api/service-ui-links`이며 읽기 전용입니다. React 화면의 링크는
이 API가 반환하는 설정에 따라 표시되므로 **기존에 39번을 설치했다면 React 코드를 다시
패치할 필요가 없습니다.**
