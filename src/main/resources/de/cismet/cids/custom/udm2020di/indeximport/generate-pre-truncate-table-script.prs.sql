SELECT 'ALTER TABLE '||a.owner||'.'||a.table_name||' DISABLE CONSTRAINT '||a.constraint_name||';'
FROM all_constraints a,
     all_constraints b
WHERE a.constraint_type = 'R'
  AND a.r_constraint_name = b.constraint_name
  AND a.r_owner = b.owner
  AND b.table_name = ?
  --AND b.table_name IN ('BORIS_SITE', 'BORIS_SAMPLE_VALUE', 'JT_BORIS_SITE_TAG', 'JT_BORIS_SSV', 'TAG', 'GEOM')