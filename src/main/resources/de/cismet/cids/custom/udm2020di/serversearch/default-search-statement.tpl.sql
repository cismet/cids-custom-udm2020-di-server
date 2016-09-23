SELECT ocid,
  oid,
  stringrep,
  geometry,
  lightweight_json
FROM
  (SELECT cs_attr_object_derived.class_id ocid,
    cs_attr_object_derived.object_id oid,
    cs_cache.stringrep,
    /*cs_cache.geometry,*/
    geom.geo_field geometry,
    cs_cache.lightweight_json,
    row_number() over (partition BY cs_attr_object_derived.class_id, cs_attr_object_derived.object_id order by rownum) rn
  FROM geom,
    CSX_OBJECT_TAG,
    cs_attr_object_derived
  LEFT OUTER JOIN cs_cache
  ON ( cs_cache.class_id        = cs_attr_object_derived.class_id
  AND cs_cache.object_id        = cs_attr_object_derived.object_id )
  WHERE CSX_OBJECT_TAG.CLASS_ID = cs_attr_object_derived.class_id
  AND CSX_OBJECT_TAG.OBJECT_ID  = cs_attr_object_derived.object_id
  AND CSX_OBJECT_TAG.TAG_ID IN 
    (SELECT ID FROM TAG WHERE TAG.key IN (%TAG_KEYS%))
  AND cs_attr_object_derived.attr_class_id =
    (SELECT cs_class.id FROM cs_class WHERE cs_class.table_name = 'GEOM')
  AND cs_attr_object_derived.attr_object_id = geom.id
  AND cs_attr_object_derived.class_id IN (SELECT ID FROM CS_CLASS WHERE NAME IN (%CLASS_NAMES%))
  AND sdo_relate(geom.geo_field, sdo_geometry('%GEOMETRY%', 4326), 'mask=anyinteract') = 'TRUE'
  ORDER BY 1,
    2,
    3
  )
WHERE rn = 1;