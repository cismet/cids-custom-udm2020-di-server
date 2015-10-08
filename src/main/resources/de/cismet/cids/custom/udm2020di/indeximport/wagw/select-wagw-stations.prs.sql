SELECT MESSSTELLEN.PK MESSSTELLE_PK, MESSSTELLEN.NAME MESSSTELLE_NAME, MESSSTELLEN.TYP MESSSTELLE_TYP, 
       MESSSTELLEN.XKOORDINATE, MESSSTELLEN.YKOORDINATE, 
       ZUSTAENDIGESTELLE ZUSTAENDIGE_STELLE, /*, BUNDESLAND,*/ GWK_NAME,
    XMLSerialize(CONTENT XMLRoot(XMLELEMENT("MESSSTELLE",  
        XMLATTRIBUTES(MESSSTELLEN.PK pk, 
            MESSSTELLEN.NAME name, 
            MESSSTELLEN.TYP typ, 
            MESSSTELLEN.STATUS status, 
            MESSSTELLEN.MESSSTELLENART messtellenart, 
            MESSSTELLEN.NUTZUNGSART nutzungsart, 
            MESSSTELLEN.ERSATZFUER ersatzfuer,
            MESSSTELLEN.ERSETZTDURCH ersetztdurch,
            MESSSTELLEN.TIEFE tiefe,
            MESSSTELLEN.XKOORDINATE xkoordinate,
            MESSSTELLEN.YKOORDINATE ykoordinate,
            MESSSTELLEN.ZUSTAENDIGESTELLE zustaendigestelle,
            MESSSTELLEN.BUNDESLAND bundesland,
            MESSSTELLEN.GWK_NAME gwkname),
(SELECT XMLELEMENT("proben", xmlagg (xmlelement ("probe", V_WAGW_PROBE_D.PK
    /*XMLATTRIBUTES(V_WAGW_PROBE_D.PK probePk, V_WAGW_PROBE_D.PROBEDATUM_DATE probeDatum)*/))) AS PROBEN 
            FROM V_WAGW_PROBE_D  
            WHERE V_WAGW_PROBE_D.FK_V_WAGW_MST_D_ID = MESSSTELLEN.PK)  AS PROBEN,
(SELECT XMLELEMENT("probenparameter",
    xmlagg (xmlelement ("PROBENPARAMETER", XMLATTRIBUTES(PROBENPARAMETER.PARAMETER_PK parameterPk, 
    /*PROBENPARAMETER.PARAMETERGRUPPE_PK parametergruppePk, PROBENPARAMETER.PARAMETERGRUPPE_NAME parametergruppeName, */
    PROBENPARAMETER.PARAMETER_NAME parameterName, PARAMETER_EINHEIT parametereinheit)))) AS PROBENPARAMETER
FROM
    (SELECT DISTINCT V_WAGW_MST_D.PK AS MESSTELLE_PK,
        V_WAGW_PARAM_D.PK AS PARAMETER_PK,
        V_WAGW_PARAM_D.NAME_TXT AS PARAMETER_NAME,
        V_WAGW_PARAM_D.UNIT_TXT AS PARAMETER_EINHEIT
        --,V_WAGW_PARAMGRUPPE_D.PK AS PARAMETERGRUPPE_PK,
        --V_WAGW_PARAMGRUPPE_D.NAME_TXT PARAMETERGRUPPE_NAME
    FROM V_WAGW_MST_D
    INNER JOIN V_WAGW_PROBE_D ON V_WAGW_PROBE_D.FK_V_WAGW_MST_D_ID = V_WAGW_MST_D.PK
    INNER JOIN V_WAGW_PARAMWERT_F ON V_WAGW_PARAMWERT_F.FK_V_WAGW_PROBE_D_ID = V_WAGW_PROBE_D.PK
    INNER JOIN V_WAGW_PARAM_D ON V_WAGW_PARAMWERT_F.FK_V_WAGW_PARAM_D_ID = V_WAGW_PARAM_D.PK
    INNER JOIN V_WAGW_BRIDGEPARAMGRP_D ON V_WAGW_BRIDGEPARAMGRP_D.FK_V_WAGW_PARAM_D_ID = V_WAGW_PARAM_D.PK
    INNER JOIN V_WAGW_PARAMGRUPPE_D  ON V_WAGW_BRIDGEPARAMGRP_D.FK_V_WAGW_PARAMGRUPPE_D_ID = V_WAGW_PARAMGRUPPE_D.PK
    WHERE V_WAGW_PARAMGRUPPE_D.PK IN('2', 
        '3', 
        '4', 
        '5', 
        '6', 
        '7', 
        '8', 
        '9', 
        '10', 
        '29', 
        '30', 
        '31', 
        '33', 
        '34', 
        '35', 
        '37', 
        '38', 
        '39', 
        '40', 
        '4028f0f02c2f4701012c3f4451c83814', 
        '4028f0f02525541201252ab815da4cf2', 
        '4028f0f0272dd7070127952bbb4470c6', 
        '8ae5e2f31a1594d3011a1594e7750034', 
        '8ae5e2f31a1594d3011a1594e7750035', 
        '8ae5e2f31a1594d3011a1594e7750036', 
        '8ae5e2f31c416081011c41608ea60001', 
        '8a20a2b93af50207013b2d10545a1002', 
        '8a20a2b93cf249d8013cf7d2e89a252e', 
        '8a20a2b93cf249d8013cf7d6a665254a', 
        '8a20a2b93cf249d8013cf7d98a7f2559', 
        '8a20a2b93cf249d8013cf7eda50e2581', 
        '8a20a2b93cf249d8013cf7e098942567', 
        '8a20a2b93cf249d8013cf7e732ea2578', 
        '8a20a2b93cf249d8013cf7fd57cc2591', 
        '8a20a2b93cf249d8013cf7ffda1e259a', 
        '8a20a2b93cf249d8013cf81c41f025de', 
        '8a20a2b93cf249d8013cf8268cf725eb', 
        '8a20a2b932f696d201335a1c83a45191', 
        '8a20a2b932f696d201335a1d01945192', 
        '8a20a2b932f696d201335a1d74e25193', 
        '8a20a2b935e928620135f1ca3d3a0767', 
        '8a20a2b94e1cfd40014ef2dc4d0a1391')
        AND V_WAGW_PARAMWERT_F.WERT_NUM IS NOT NULL
    GROUP BY V_WAGW_MST_D.PK, V_WAGW_PARAM_D.PK, V_WAGW_PARAM_D.NAME_TXT, V_WAGW_PARAM_D.UNIT_TXT
    --,V_WAGW_PARAMGRUPPE_D.PK, V_WAGW_PARAMGRUPPE_D.NAME_TXT
    ORDER BY MESSTELLE_PK, PARAMETER_PK) PROBENPARAMETER
WHERE PROBENPARAMETER.MESSTELLE_PK = MESSSTELLEN.PK
GROUP BY PROBENPARAMETER.MESSTELLE_PK) AS PROBENPARAMETER), VERSION '1.0' , STANDALONE YES))  AS MESSSTELLE_XML
FROM (
    SELECT DISTINCT MESSSTELLE.PK AS PK,
    MESSSTELLE.NAME_TXT AS NAME,
    MESSSTELLE.TYPE_TXT AS TYP,
    MESSSTELLE.STATUS_TXT AS STATUS, 
    MESSSTELLE.MESSSTELLENART_TXT AS MESSSTELLENART, 
    MESSSTELLE.NUTZUNGSART_TXT AS NUTZUNGSART, 
    MESSSTELLE.ERSATZFUERMESSSTELLE_TXT AS ERSATZFUER,
    MESSSTELLE.ERSETZTDURCHMESSSTELLE_TXT AS ERSETZTDURCH,
    MESSSTELLE.MSTTIEFE_NUM AS TIEFE,
    MESSSTELLE.XKOORDINATE_NUM AS XKOORDINATE,
    MESSSTELLE.YKOORDINATE_NUM AS YKOORDINATE,
    MAX(ZUSTAENDIGESTELLE.NAME_TXT) AS ZUSTAENDIGESTELLE, 
    MAX(PROVINCE.NAME_TXT) AS BUNDESLAND,
    MAX(GWK.NAME_TXT) AS GWK_NAME
    FROM V_WAGW_MST_D MESSSTELLE
    INNER JOIN V_WA_ZUSTAENDIGESTELLE_D ZUSTAENDIGESTELLE ON ZUSTAENDIGESTELLE.PK = MESSSTELLE.FK_V_WA_ZUSTAENDIGESTELLE_D_ID
    INNER JOIN V_PROVINCE_HD PROVINCE ON PROVINCE.PK = MESSSTELLE.FK_V_PROVINCE_HD_ID 
    INNER JOIN V_WAGW_GWKOERPER_D GWK ON MESSSTELLE.FK_V_WAGW_GWKOERPER_D_ID = GWK.PK
    INNER JOIN V_WAGW_PROBE_D PROBE ON PROBE.FK_V_WAGW_MST_D_ID = MESSSTELLE.PK
    INNER JOIN V_WAGW_PARAMWERT_F PARAMWERT ON PARAMWERT.FK_V_WAGW_PROBE_D_ID = PROBE.PK
    INNER JOIN V_WAGW_PARAM_D PARAMETER ON PARAMWERT.FK_V_WAGW_PARAM_D_ID = PARAMETER.PK
    INNER JOIN V_WAGW_BRIDGEPARAMGRP_D BRIDGEPARAMGRP ON BRIDGEPARAMGRP.FK_V_WAGW_PARAM_D_ID = PARAMETER.PK
    INNER JOIN V_WAGW_PARAMGRUPPE_D PARAMETERGRUPPE ON BRIDGEPARAMGRP.FK_V_WAGW_PARAMGRUPPE_D_ID = PARAMETERGRUPPE.PK
    WHERE SDO_GEOM.RELATE(
        sdo_geometry('Polygon ((541379 523462, 541379 423907, 657834 423907, 657834 523462, 541379 523462))',31287),
        'contains', MESSSTELLE.MST_GPT, 0.10) = 'CONTAINS'
    AND PARAMETERGRUPPE.PK IN('2', 
        '3', 
        '4', 
        '5', 
        '6', 
        '7', 
        '8', 
        '9', 
        '10', 
        '29', 
        '30', 
        '31', 
        '33', 
        '34', 
        '35', 
        '37', 
        '38', 
        '39', 
        '40', 
        '4028f0f02c2f4701012c3f4451c83814', 
        '4028f0f02525541201252ab815da4cf2', 
        '4028f0f0272dd7070127952bbb4470c6', 
        '8ae5e2f31a1594d3011a1594e7750034', 
        '8ae5e2f31a1594d3011a1594e7750035', 
        '8ae5e2f31a1594d3011a1594e7750036', 
        '8ae5e2f31c416081011c41608ea60001', 
        '8a20a2b93af50207013b2d10545a1002', 
        '8a20a2b93cf249d8013cf7d2e89a252e', 
        '8a20a2b93cf249d8013cf7d6a665254a', 
        '8a20a2b93cf249d8013cf7d98a7f2559', 
        '8a20a2b93cf249d8013cf7eda50e2581', 
        '8a20a2b93cf249d8013cf7e098942567', 
        '8a20a2b93cf249d8013cf7e732ea2578', 
        '8a20a2b93cf249d8013cf7fd57cc2591', 
        '8a20a2b93cf249d8013cf7ffda1e259a', 
        '8a20a2b93cf249d8013cf81c41f025de', 
        '8a20a2b93cf249d8013cf8268cf725eb', 
        '8a20a2b932f696d201335a1c83a45191', 
        '8a20a2b932f696d201335a1d01945192', 
        '8a20a2b932f696d201335a1d74e25193', 
        '8a20a2b935e928620135f1ca3d3a0767', 
        '8a20a2b94e1cfd40014ef2dc4d0a1391')
    --AND ROWNUM < 100
    GROUP BY MESSSTELLE.PK, MESSSTELLE.NAME_TXT, MESSSTELLE.TYPE_TXT, MESSSTELLE.STATUS_TXT, 
        MESSSTELLE.MESSSTELLENART_TXT, MESSSTELLE.NUTZUNGSART_TXT, 
        MESSSTELLE.ERSATZFUERMESSSTELLE_TXT, MESSSTELLE.ERSETZTDURCHMESSSTELLE_TXT, 
        MESSSTELLE.MSTTIEFE_NUM,MESSSTELLE.XKOORDINATE_NUM , MESSSTELLE.YKOORDINATE_NUM
    ORDER BY MESSSTELLE.PK) MESSSTELLEN