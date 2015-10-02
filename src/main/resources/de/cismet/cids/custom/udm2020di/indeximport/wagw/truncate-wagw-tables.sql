ALTER SESSION SET ddl_lock_timeout=360;
DELETE FROM GEOM WHERE ID IN (SELECT WAGW_STATION.GEOMETRY FROM WAGW_STATION);
DELETE FROM TAG WHERE ID IN (SELECT WAGW_STATION.ZUSTAENDIGE_STELLE FROM WAGW_STATION);
DELETE FROM TAG WHERE ID IN (SELECT WAGW_STATION.GWK_NAME FROM WAGW_STATION);
TRUNCATE TABLE JT_WAGW_STT;
TRUNCATE TABLE WAGW_STATION;
TRUNCATE TABLE WAGW_SAMPLE_VALUE;