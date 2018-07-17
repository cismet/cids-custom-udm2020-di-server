SELECT ocid,
      oid,
      stringrep,
      geometry,
      lightweight_json
FROM
      (SELECT cs_attr_object_derived.class_id ocid,
        cs_attr_object_derived.object_id oid,
        CSX_CACHE.stringrep,
        CSX_CACHE.geometry,
        CSX_CACHE.lightweight_json,
        row_number() 
        OVER (partition BY cs_attr_object_derived.class_id, cs_attr_object_derived.object_id 
        ORDER BY
                cs_attr_object_derived.class_id,
                cs_attr_object_derived.object_id) part      
      FROM geom,
        CSX_OBJECT_TAG_TIMESTAMP,
        cs_attr_object_derived
      LEFT OUTER JOIN CSX_CACHE
      ON ( CSX_CACHE.class_id = cs_attr_object_derived.class_id
      AND CSX_CACHE.object_id = cs_attr_object_derived.object_id )
      WHERE CSX_OBJECT_TAG_TIMESTAMP.CLASS_ID = cs_attr_object_derived.class_id
      AND CSX_OBJECT_TAG_TIMESTAMP.OBJECT_ID = cs_attr_object_derived.object_id
      AND CSX_OBJECT_TAG_TIMESTAMP.TAG_ID IN
        (SELECT ID FROM TAG WHERE TAG.key IN (%TAG_KEYS%))
      AND cs_attr_object_derived.attr_class_id =
        (SELECT cs_class.id FROM cs_class WHERE cs_class.table_name = 'GEOM')
      AND cs_attr_object_derived.attr_object_id = geom.id
      AND cs_attr_object_derived.class_id      IN
        (SELECT ID FROM CS_CLASS WHERE CS_CLASS.NAME IN (%CLASS_NAMES%))
      AND st_contains(GeomFromEWKT(?),geom.geo_field)
      ORDER BY ocid, oid) AS foo
WHERE part = 1
LIMIT ?