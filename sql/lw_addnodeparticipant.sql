/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_addnodeparticipant(
    lw_schema text,
    nodeinfo json
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_addnodeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
 
BEGIN
EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'NODE', %4$L)$$,
    lw_schema, nodeinfo->>'schemaname',nodeinfo->>'tablename', nodeinfo
);
END;
$lw_addnodeparticipant$;