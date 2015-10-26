SELECT MAX(AL_CONV) AS "AL",
       MAX(AS_CONV) AS "AS",
       MAX(CD_CONV) AS "CD",
       MAX(CO_CONV) AS "CO",
       MAX(CR_CONV) AS "CR",
       MAX(CU_CONV) AS "CU",
       MAX(FE_CONV) AS "FE",
       MAX(MO_CONV) AS "MO",
       MAX(NI_CONV) AS "NI",
       MAX(PB_CONV) AS "PB",
       MAX(S_CONV) AS "S",
       MAX(V_CONV) AS "V",
       MAX(SB_CONV) AS "SB",
       MAX(ZN_CONV) AS "ZN",
       MAX(HG_CONV) AS "HG",
       MAX(N_TOTAL)  AS "N"
FROM MOSS
WHERE MOSS.ID IN (%OBJECT_IDS%)