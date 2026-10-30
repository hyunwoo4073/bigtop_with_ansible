-- Managed by Ansible Step 44. No DROP and no base-table writes.
-- Execute ONLY after the role preflight verifies the view name is unused.
DO $bigtop_lineage$
BEGIN
  EXECUTE $bigtop_view$
    CREATE VIEW public.v_bigtop_service_inventory_lineage AS
    SELECT
      s.cluster_slug,
      s.host_name,
      h.inventory_address,
      s.service_name,
      s.component_name,
      s.group_name
    FROM public.platform_service_instances AS s
    INNER JOIN public.platform_hosts AS h
      ON h.cluster_slug = s.cluster_slug
     AND h.host_name = s.host_name
    WHERE s.is_present = TRUE AND h.is_present = TRUE
  $bigtop_view$;
  EXECUTE $bigtop_comment$
    COMMENT ON VIEW public.v_bigtop_service_inventory_lineage
      IS 'bigtop-governance-lineage:step44:v1'
  $bigtop_comment$;
END
$bigtop_lineage$;
