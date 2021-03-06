SELECT TAG.ID,
       TAG.KEY,
       TAG.NAME,
       TAG.DESCRIPTION,
       TAG.TAGGROUP AS TAGGROUPID,
       TAG.TAGGROUP_KEY AS TAGGROUPKEY
FROM TAG
INNER JOIN JT_MOSS_TAG ON JT_MOSS_TAG.TAG = TAG.ID
AND JT_MOSS_TAG.MOSS = :MOSS_ID
ORDER BY TAG.TAGGROUP_KEY,
         TAG.KEY
