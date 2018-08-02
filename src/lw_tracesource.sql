CREATE OR REPLACE FUNCTION lw_tracesource(
    lw_schema text,
    source bigint,
    distance bigint default 37500
  )
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_tracesource$

DECLARE
  qrytxt text;
  srid int;
  looprec record;
BEGIN

EXECUTE format('delete from %I.livewire where nodes[1] = %s',lw_schema,source);

  /*    Trace from source to all blocks    */
  qrytxt := $_$
		INSERT into %1$I.livewire
        select  
          array_agg(node order by path_seq) nodes ,
          array_remove(array_agg(edge order by path_seq),-1::bigint) edges
        from pgr_dijkstra(
        	 $$select lw_id  id, source, target, st_length(g)  as cost  
        	 from %1$I.lines
        	 where lines.g && st_expand(
        	  (select nodes.g from %1$I.nodes where nodes.lw_id = %2$s)
        	  ,%3$s)
        	 $$,
        	 array[%2$s]::bigint[],
        	 (select lw_blocknodes('%1$s')),
        	 
        	 false
        	 )
        join %1$I.nodes on lw_id = node
        group by start_vid, end_vid 
        having True =ALL ((array_agg(
          CASE WHEN status = 'BLOCK' THEN False ELSE True END order by path_seq desc
          ))[2:] 
        )
  $_$;
  EXECUTE format(qrytxt,lw_schema, source, distance);  
  
  -- check if nearby block nodes only have one entry in livewire
  FOR looprec in EXECUTE 
        format($$with a as (select unnest( lw_blocknodes('%1$s',%2$s,500)) aa)
		   select aa,count(aa) from a left join gratis.livewire 
		   on aa = nodes[array_position(nodes,aa)] group by aa 
		   having count(aa) < 2$$,lw_schema, source) LOOP
		   raise notice '%', looprec;
		   
	qrytxt := $_$
	    INSERT into %1$I.livewire
      select  
      	array_agg(node order by path_seq desc), 
      	array_agg(edge order by path_seq desc)
      from pgr_ksp( 
      	$$select lw_id::int4 id, source::int4, target::int4, st_length(g)::float8 as cost 
      	from  %1$I.lines$$, 
      	%3$s, 
      	%2$s, 
      	9,
      	directed:= false ) 
      	join %1$I.nodes on lw_id = node
      	group by path_id
      	having true =all ((array_agg (CASE WHEN status = 'BLOCK' THEN False ELSE True END))[2:])
		$_$;
		 EXECUTE format(qrytxt,lw_schema, source, looprec.aa);  
		   
		   
	END LOOP;
  
  
  
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