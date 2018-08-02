CREATE OR REPLACE FUNCTION lw_addedgeparticipant(
    lw_schema text,
    edgeinfo json
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_addedgeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
 
BEGIN
EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'EDGE', %4$L)$$,
    lw_schema, edgeinfo->>'schemaname',edgeinfo->>'tablename', edgeinfo
);
END;
$lw_addedgeparticipant$;