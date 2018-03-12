CREATE OR REPLACE FORCE VIEW "UIM_DI01"."JT_MOSS_TAG" ("ID", "TAG", "MOSS") AS 
  SELECT rownum AS ID,
       TAG.ID AS TAG,
       MOSS.ID AS MOSS
FROM TAG,
     MOSS
WHERE TAG.KEY IN ('Al',
                  'As',
                  'Cd',
                  'Co',
                  'Cr',
                  'Cu',
                  'Fe',
                  'Mo',
                  'Ni',
                  'Pb',
                  'S',
                  'V',
                  'Sb',
                  'Sn',
                  'Hg',
                  'N')
  AND TAG.TAGGROUP_KEY = 'POLLUTANT'
UNION
SELECT rownum AS ID,
       TAG.ID AS TAG,
       MOSS.ID AS MOSS
FROM TAG,
     MOSS
WHERE TAG.KEY IN ('MET',
                  'DNM')
  AND TAG.TAGGROUP_KEY = 'POLLUTANTGROUP';
 
