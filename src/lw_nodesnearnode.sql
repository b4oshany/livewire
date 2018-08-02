CREATE OR REPLACE FUNCTION lw_nodesnearnode(
    in lw_schema text,
    in source bigint,
    in distance bigint,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_nodesnearnode$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes near a given node
  qrytxt := 'select array_agg(lw_id)::bigint[] from (
              select nodes.lw_id from %1$I.nodes
              join %1$I.lines on st_dwithin(nodes.g,lines.g,.001)
              where lines.g && (
                select st_expand(nodes.g,%3$L)
                from %I.nodes where nodes.lw_id = %2$L
                )
              group by nodes.lw_id
              having count(lines.lw_id) = 1) 
            as foo)';
  execute(qrytxt,lw_schema, source, distance) into myarray;

END;
AS $lw_nodesnearnode$;