/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
    in lw_schema text,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_sourcenodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := $$select array_agg(lw_id) from %1$I.nodes
		where status = 'SOURCE'$$;
  
  execute format(qrytxt,lw_schema) into myarray;
END;
 $lw_sourcenodes$;