select nodes.lw_id, nodes.g from burlap.nodes 
join burlap.lines on st_dwithin(nodes.g,lines.g,.001)
where lines.g  && st_expand((select nodes.g from burlap.nodes where lw_id = 9920),25000)
group by nodes.lw_id
having count(lines.lw_id) = 1




/* get nearest x amount of deadend nodes to a given point*/
select lw_id from (
  select nodes.lw_id from burlap.nodes 
  join burlap.lines on st_dwithin(nodes.g,lines.g,.001)
  order by (select nodes.g from burlap.nodes where lw_id = 9920) <-> nodes.g limit 20000) as foo
group by lw_id
having count(lw_id) = 1



/* redirect */

CREATE OR REPLACE FUNCTION nt.redirect(
	source bigint,
	batchsize bigint DEFAULT 500)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE
looprec record;
outerlooprec record;
cnt integer;
pencil boolean;
marker boolean;

BEGIN
cnt := 0;
for outerlooprec in select facilityid::bigint from livewire.transformerbank order by (select g from livewire.isolators where facilityid = source::text)<-> g limit batchsize  LOOP
 raise notice 'Looking for: %',outerlooprec.facilityid;
for looprec in SELECT  qq.*, st_length(g) 
      FROM pgr_dijkstra(
      'SELECT fid AS id, source::int AS source, target::int AS target, st_length(g) AS cost FROM nt.lines ', 
      array[source]::bigint[],
      array[outerlooprec.facilityid]::bigint[], 
      false) qq
    JOIN nt.lines on fid = edge LOOP
    --raise notice '%', looprec;
	--raise notice 'lineid: % | nodeid: %', looprec.node,looprec.edge;
    select into marker status = 'BLOCK' from nt.nodes where fid = looprec.node;
	if marker = true then
		RAISE NOTICE 'Found open switch at %', looprec.node;
		EXIT;
	END IF;
	-- select into pencil st_equals(n.g,st_startpoint(l.g)) from nt.lines l,nt.nodes n
    -- where st_intersects(n.g,l.g) and n.fid = looprec.node and l.fid = looprec.edge;
	select into pencil st_distance(n.g,st_startpoint(l.g)) < .005 from nt.lines l,nt.nodes n
    where st_dwithin(n.g,l.g,.005) and n.fid = looprec.node and l.fid = looprec.edge;
	if pencil = false then
		update nt.lines set g = st_reverse(g) where fid = looprec.edge;
		raise notice 'Flipped %', looprec.edge;
	end if;
    cnt = cnt + 1;
    /*if cnt > 15 then
    	EXIT;
    end if;*/
end loop;
end loop;
return;

END;

$BODY$;
