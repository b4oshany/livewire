/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_tracesource(
    in lw_schema text,
    in source bigint,
    in distance bigint default 37500,
    in checkzero boolean default true
  )
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_tracesource$

DECLARE
  closeblock bigint;
  closeblocks bigint[];
  qrytxt text;
  zerocount bigint;

BEGIN

EXECUTE format('delete from %I.livewire where nodes[1] = %s',lw_schema,source);

if checkzero = True THEN

/*    Verify that this source cannot reach other sources....that would be bad   */
  qrytxt := $_$
    select count(*) from pgr_dijkstra(
      $$select lw_id  id, source, target, st_length(g) * multiplier as cost  
      from %1$I.lines  $$,
      %2$s, 
      (select array_agg(lw_id) from %1$I.nodes where status = 'SOURCE'),
      false
    )
  $_$;
  EXECUTE format(qrytxt,lw_schema, source) into zerocount; 
  if zerocount > 0 THEN
    raise notice 'Zerocount is not zero!!';
  END IF;


end if;
 
 
  
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


  /*    Find blocks within 20km of current extent of feeder. Trace from found blocks to source.   */
 
  qrytxt := $_$
    select array_agg(lw_id) from %1$I.nodes 
    where status = 'BLOCK' and g && (
      select st_expand(st_collect(g),2000) 
      from %1$I.lines where lw_id in (
        select unnest(edges) from %1$I.livewire where nodes[1] =  %2$s
        )
    )
  $_$;

qrytxt:= $_$
  select array_agg(lw_id) from (
  select lw_id from %1$I.nodes
  where status = 'BLOCK'
  order by g <-> (
      select st_collect(g)
      from %1$I.lines where lw_id in (
        select unnest(edges) from %1$I.livewire where nodes[1] =  %2$s
        ))
    limit 10) as foo$_$;


  execute format(qrytxt,lw_schema,source) into closeblocks;

  foreach closeblock in array closeblocks loop
    qrytxt := $_$
      INSERT into %1$I.livewire
      select 
      array_agg(node order by path_seq) nodes ,
        array_remove(array_agg(edge order by path_seq),-1::bigint) edges
      from pgr_dijkstra(
      $$select lw_id  id, source, target, 
      st_length(g) * case when %3$s in (source,target) then 1 else multiplier end  as cost  
        from %1$I.lines
        $$,
        array[%2$s]::bigint[],
        array[%3$s]::bigint[],
        false
        )
        join %1$I.nodes on lw_id = node
      group by start_vid, end_vid $_$;
    execute format(qrytxt,lw_schema,source, closeblock);
  END LOOP;





END;
$lw_tracesource$;