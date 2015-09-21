SELECT STANDORT_PK,
  (case when PARAMETER_PK IN ('B424', 'B405') then 'B404' 
        when PARAMETER_PK = 'B418' then 'B407'
        when PARAMETER_PK = 'B440' then 'B430'
        when PARAMETER_PK IN ('B442', 'B443', 'B444') then 'B432' 
        when PARAMETER_PK = 'B444' then 'B433'
        when PARAMETER_PK = 'B446' then 'B435'
        when PARAMETER_PK IN ('B449','B450','B451','B452') then 'B437' 
        when PARAMETER_PK IN ('B453', 'B454') then 'B438' 
        when PARAMETER_PK IN ('B431','B441') then 'B455' 
        when PARAMETER_PK IN ('B436','B447','B448') then 'B456' 
        when PARAMETER_PK IN ('B467','B463','B464','B468','B465','B460','B461','B466','B46A') then 'B462' 
        else PARAMETER_PK end) PARAMETER_PK,
  --rtrim(xmlagg (xmlelement (e, PARAMETER_PK || ',')).extract ('//text()'), ',') PARAMETER_PKS,
  --PROBEN_PKS,
  MIN(MIN_DATE) AS MIN_DATE,
  MAX(MAX_DATE) AS MAX_DATE,
  MIN(MIN_VALUE) AS MIN_VALUE,
  MAX(MAX_VALUE) AS MAX_VALUE,
  -- parameter specific normalisation computation
  /*MIN(case when PARAMETER_PK = 'B434' then 0 
      else MIN_VALUE end) AS MIN_VALUE,
  MAX(case when PARAMETER_PK = 'B434' then 0 
      else MAX_VALUE end) AS MAX_VALUE,*/
  XMLSerialize(CONTENT XMLRoot(xmlelement("messwerte", XMLATTRIBUTES(STANDORT_PK standortPk, PROBEN_PKS probenPks), 
    --XMLParse(CONTENT PROBEN_XML WELLFORMED),
    xmlagg (xmlelement ("messwerte", XMLATTRIBUTES(PARAMETER_PK parameterPk, PARAMETER_NAME parameterName, 
       MAX_DATE maxDate, MIN_DATE minDate, MIN_VALUE minValue, MAX_VALUE maxValue)))), 
  VERSION '1.0', STANDALONE YES)) MESSWERTE_XML
FROM (SELECT DISTINCT T_BIS_STANDORT_D.PK AS STANDORT_PK,
          --T_BIS_MESSWERT_F.PK AS BIS_MESSWERT_FK,
          rtrim(xmlagg (xmlelement (PROBEN, T_BIS_PROBE_B.PK || ',')).extract ('//text()'), ',') PROBEN_PKS,
          --xmlagg (xmlelement (PROBEN,  XMLATTRIBUTES(T_BIS_PROBE_B.PK AS PROBE_PK))).getStringVal()  PROBEN_XML,
          T_BIS_PARAMETER_B.PK AS PARAMETER_PK,
          T_BIS_PARAMETER_B.TEXT_TXT PARAMETER_NAME,
          MAX(TO_DATE(T_BIS_PROBE_B.PROBENAHMEDATUM_TXT, 'YYYYMMDD')) AS MAX_DATE,
          MIN(TO_DATE(T_BIS_PROBE_B.PROBENAHMEDATUM_TXT, 'YYYYMMDD')) AS MIN_DATE,
          MIN(T_BIS_MESSWERT_F.WERT_NUM) AS MIN_VALUE,
          MAX(T_BIS_MESSWERT_F.WERT_NUM) AS MAX_VALUE
    FROM T_BIS_MESSWERT_F
    INNER JOIN T_BIS_PARAMETER_B ON T_BIS_PARAMETER_B.PK = T_BIS_MESSWERT_F.FK_T_BIS_PARAM_B
    --AND T_BIS_PARAMETER_B.PK IN ('BCU1', 'BZN1', 'BPB1')
    INNER JOIN T_BIS_PARAMGRUPPE_D ON T_BIS_PARAMGRUPPE_D.PK = T_BIS_PARAMETER_B.FK_T_BIS_PARAMGRUPPE_D
    AND T_BIS_PARAMGRUPPE_D.PK IN ('B10000',
                                   'B30000',
                                   'B40000',
                                   'B40100',
                                   'B40200',
                                   'B40300',
                                   'B40400',
                                   'B40500',
                                   'B40600',
                                   'B50000',
                                   'B70000')
    INNER JOIN T_BIS_PROBE_B ON T_BIS_PROBE_B.PK = T_BIS_MESSWERT_F.FK_T_BIS_PROBE_B
    INNER JOIN T_BIS_STANDORT_D ON T_BIS_STANDORT_D.PK = T_BIS_PROBE_B.FK_T_BIS_STANDORT_D
    WHERE T_BIS_PROBE_B.FK_T_BIS_STANDORT_D = ?
    -- AND ROWNUM <= 10000
    GROUP BY T_BIS_STANDORT_D.PK,
             --T_BIS_MESSWERT_F.PK,
             T_BIS_PARAMETER_B.PK,
             T_BIS_PROBE_B.PROBENAHMEDATUM_TXT,
             T_BIS_PARAMETER_B.TEXT_TXT
    ORDER BY T_BIS_STANDORT_D.PK,
             T_BIS_PARAMETER_B.PK)
GROUP BY STANDORT_PK, PROBEN_PKS, /*PROBEN_XML,*/ 
  (case when PARAMETER_PK IN ('B424', 'B405') then 'B404' 
        when PARAMETER_PK = 'B418' then 'B407'
        when PARAMETER_PK = 'B440' then 'B430'
        when PARAMETER_PK IN ('B442', 'B443', 'B444') then 'B432' 
        when PARAMETER_PK = 'B444' then 'B433'
        when PARAMETER_PK = 'B446' then 'B435'
        when PARAMETER_PK IN ('B449','B450','B451','B452') then 'B437' 
        when PARAMETER_PK IN ('B453', 'B454') then 'B438' 
        when PARAMETER_PK IN ('B431','B441') then 'B455' 
        when PARAMETER_PK IN ('B436','B447','B448') then 'B456' 
        when PARAMETER_PK IN ('B467','B463','B464','B468','B465','B460','B461','B466','B46A') then 'B462' 
        else PARAMETER_PK end)