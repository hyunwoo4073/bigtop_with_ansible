# Step 50 one-page deployment

On worker3, after copying `50_olist_openmetadata_ansible.zip` into the project:

```bash
cd ~/bigtop-cluster-ansible
unzip -o 50_olist_openmetadata_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_catalog.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml
# Verify plan creates 16 containers + 13 edge proposals if empty.
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_apply=true
```

Expected when first applied to empty namespace: `OLIST CATALOG CHANGED: 30 create operations`
(1 service, 16 containers, 13 lineage edges). On subsequent calls:
`OLIST CATALOG UNCHANGED: 0 create operations`.

Full explanation: `docs/50-olist-openmetadata.md`.
