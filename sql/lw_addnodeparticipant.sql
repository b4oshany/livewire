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

IF lw_schema = nodeinfo->>'schemaname' THEN
  -- data already exists in lw_schema. Nothing to see here, move along.
  EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig)
    VALUES ('%2$I.%3$I', 'NODE', %4$L)$$,
    lw_schema, nodeinfo->>'schemaname',nodeinfo->>'tablename', nodeinfo);

ELSE
  -- data needs to be copied over eh?
  -- Lets do that here... we need to use create table like semantics to copy over the table structure.
  -- in the mean time, lets raise an error saying we don't support this yet
  RAISE EXCEPTION 'Table not in schema. buh bye.';
END IF;

END;
$lw_addnodeparticipant$
