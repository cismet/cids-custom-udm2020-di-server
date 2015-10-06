INSERT INTO WAGW_STATION (
    "KEY", 
    "NAME", 
    DESCRIPTION, 
    GEOMETRY, 
    ZUSTAENDIGE_STELLE, 
    GWK_NAME, 
    TAGS, 
    SAMPLE_VALUES,
    SRC_MESSSTELLE_PK, 
    SRC_CONTENT) 
VALUES( 
    :KEY,
    :NAME,
    :DESCRIPTION,
    :GEOMETRY, 
    (SELECT ID FROM TAG WHERE KEY = :ZUSTAENDIGE_STELLE AND TAGGROUP_KEY='WAGW.ZUSTAENDIGE_STELLE'),  
    (SELECT ID FROM TAG WHERE KEY = :GEW_NAME AND TAGGROUP_KEY='WAGW.GWK'),  
    -- DOES NOT WORK!!!!!!! CURRVAL IS NOT DEFINED, NEXVTAL = ID-- 
    -- WAGW_STATION_ID_SEQ.CURRVAL TAGS, 
    -- WAGW_STATION_ID_SEQ.CURRVAL SAMPLE_VALUES, 
    (SELECT MAX(ID)+1 FROM WAGW_STATION), 
    (SELECT MAX(ID)+1 FROM WAGW_STATION), 
    :SRC_MESSSTELLE_PK, 
    :SRC_CONTENT)


