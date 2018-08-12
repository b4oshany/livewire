/*    Returns an array of lw_ids that correspond to endnodes    */

CREATE OR REPLACE FUNCTION lw_endnodes(
    in lw_schema text,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_endnodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := 'select array_agg(lw_id) from
		(select source lw_id from 
		(select lw_id, source from %1$I.lines 
		union 
		select lw_id, target from %1$I.lines ) as lines
		group by source  
		having count(lw_id) = 1) as lw_ids';
  
  execute format(qrytxt,lw_schema) into myarray;
END;
 $lw_endnodes$;


CREATE OR REPLACE FUNCTION lw_endnodes(
    in lw_schema text,
    in source bigint,
    in distance bigint,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'
AS
 $lw_endnodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes near a given node
  
  qrytxt := 'with lines as (
		select * from %1$I.lines 
		where lines.g && (
		select st_expand(nodes.g,%3$L) from %1$I.nodes where nodes.lw_id = %2$L)
			)
		
		select array_agg(lw_id)::bigint[] from (
		select source lw_id from (
		select lw_id, source from lines
		union
		select lw_id, target from lines
			) as foo group by source having count(lw_id) = 1
			) as lw_ids';
  execute format(qrytxt,lw_schema, source, distance) into myarray;
END;
 $lw_endnodes$;
 
 
 
 
