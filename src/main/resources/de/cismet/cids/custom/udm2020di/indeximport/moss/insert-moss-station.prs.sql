INSERT INTO MOSS (
    "KEY", 
    "NAME", 
    DESCRIPTION, 
    GEOMETRY, 
    MOSS_TYPE, 
    SRC_SAMPLE_ID) 
VALUES( 
    :KEY,
    :NAME,
    :DESCRIPTION,
    :GEOMETRY, 
    (SELECT ID FROM TAG WHERE KEY = :MOSS_TYPE AND TAGGROUP_KEY='MOSS.MOSS_TYPE'),  
    :SRC_SAMPLE_ID)