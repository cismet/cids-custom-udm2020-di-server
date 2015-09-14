SELECT 'alter table '||a.owner||'.'||a.table_name||' enable constraint '||a.constraint_name||';'
FROM all_constraints a,
     all_constraints b
WHERE a.constraint_type = 'R'
  AND a.r_constraint_name = b.constraint_name
  AND a.r_owner = b.owner
  AND b.table_name = ?