/*    Gets the SRID of a livewire enabled schema    */

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
-- Check that lw_schema and edgeinfo->>schemaname are the same. If they are not, then copy the table into lw_schema.
IF lw_schema = edgeinfo->>'schemaname' THEN
  -- data already exists in lw_schema. Nothing to see here, move along.
  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'EDGE', %4$L)$$,
    lw_schema, edgeinfo->>'schemaname',edgeinfo->>'tablename', edgeinfo);

ELSE
  -- data needs to be copied over eh?
  -- Lets do that here... we need to use create table like semantics to copy over the table structure.
  -- in the mean time, lets raise an error saying we don't support this yet
  RAISE EXCEPTION 'Table not in schema. buh bye.';
END IF;


END;
$lw_addedgeparticipant$;
