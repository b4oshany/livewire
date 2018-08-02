CREATE OR REPLACE FUNCTION lw_tracesource(
    lw_schema text,
    source bigint,
    distance bigint default 17500
  )
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_tracesource$

DECLARE
  qrytxt text;
  srid int;
  nodeinfo json;
BEGIN

EXECUTE format('delete from %I.livewire where nodes[1] = %s',lw_schema,source);

  /*    Trace from all blocks to source   */
  qrytxt := $_$
		INSERT into %1$I.livewire
        select  
          array_agg(node order by path_seq desc) nodes ,
          array_remove(array_agg(edge order by path_seq desc),-1::bigint) edges
        from pgr_dijkstra(
        	 $$select lw_id  id, source, target, st_length(g) * multiplier as cost  
        	 from %1$I.lines
        	 where lines.g && st_expand(
        	  (select nodes.g from %1$I.nodes where nodes.lw_id = %2$s)
        	  ,%3$s)
        	 $$,
        	 (select array_agg(lw_id)::bigint[] from gratis.nodes where status = 'BLOCK' ),
        	 array[%2$s]::bigint[],
        	 false
        	 )
        join %1$I.nodes on lw_id = node
        group by start_vid, end_vid 
        having True =ALL ((array_agg(
          CASE WHEN status = 'BLOCK' THEN False ELSE True END order by path_seq
          ))[2:] 
        )
  $_$;
  EXECUTE format(qrytxt,lw_schema, source, distance);  
  /*    Trace from source out to distance  */
  qrytxt := $_$
		INSERT into %1$I.livewire
        select  
          array_agg(node order by path_seq) nodes ,
          array_remove(array_agg(edge order by path_seq),-1::bigint) edges
        from pgr_dijkstra(
        	 $$select lw_id  id, source, target, st_length(g) * multiplier as cost  
        	 from %1$I.lines
        	 where lines.g && st_expand(
        	  (select nodes.g from %1$I.nodes where nodes.lw_id = %2$s)
        	  ,%3$s)
        	 $$,
        	 array[%2$s]::bigint[],
        	 (select lw_endnodes('%1$s',%2$s,%3$s)),
        	 false
        	 )
        join %1$I.nodes on lw_id = node
        group by start_vid, end_vid
        having True =ALL (array_agg
          (CASE WHEN status = 'BLOCK' THEN False ELSE True END)
          )
  $_$;  
  --raise notice '%', format(qrytxt,lw_schema, source, distance);
  EXECUTE format(qrytxt,lw_schema, source, distance);

END;
$lw_tracesource$;