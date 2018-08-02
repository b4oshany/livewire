-- FUNCTION: nt.generate()

-- DROP FUNCTION nt.generate();

CREATE OR REPLACE FUNCTION nt.generate(
	)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
AS $BODY$

truncate nt.nodes;
truncate nt.lines;
insert into nt.nodes 
	with one as (
		select st_astext(st_startpoint(g)) p from livewire.primaryline 
		union
		select st_astext(st_endpoint(g)) from livewire.primaryline
	)
	select nextval('nt.livewire_seq'), null::text status, st_setsrid(p::geometry,3448) 
	from one ;
update nt.nodes set status = 'DEVICE' from nt.isolators li where st_dwithin(li.g,nodes.g,.001);
update nt.nodes set status = 'SOURCE' from nt.isolators li where st_dwithin(li.g,nodes.g,.001) and li.devicetype = 'FDR';
update nt.nodes set status = 'BLOCK' from nt.isolators li where st_dwithin(li.g,nodes.g,.001) and li.status = 'OPEN';

update nt.nodes set fid =  facilityid::bigint from nt.isolators li where st_dwithin(li.g,nodes.g,.001);
update nt.nodes set fid = facilityid::bigint, status = 'XFMR' from nt.transformerbanks li where st_dwithin(li.g,nodes.g,.001);

insert into nt.lines(fid, x1, y1, x2, y2, multiplier, g)
select globalid, st_x(st_startpoint(g)),st_y(st_startpoint(g)), st_x(st_endpoint(g)),st_y(st_endpoint(g)), 1, g 
from nt.primarylines ;

update nt.lines set source = nodes.fid from nt.nodes where st_dwithin(nodes.g, st_startpoint(lines.g),.001);
update nt.lines set target = nodes.fid from nt.nodes where st_dwithin(nodes.g, st_endpoint(lines.g),.001);
update nt.lines set multiplier = 1e18 from nt.nodes where st_dwithin(nodes.g,lines.g,.001) and nodes.status = 'BLOCK';
update nt.nodes set status = 'DEADEND' where fid in (
	select n.fid from nt.nodes n
	join nt.lines l on st_intersects(l.g,n.g) 
	where status is null
	group by n.fid
	having count(l.fid) = 1);

$BODY$;

ALTER FUNCTION nt.generate()
    OWNER TO postgres;



/***********************/

-- FUNCTION: nt.generate2()

-- DROP FUNCTION nt.generate2();

CREATE OR REPLACE FUNCTION nt.generate2(
	)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
AS $BODY$

truncate nt.nodes2;
truncate nt.lines2;

insert into nt.lines2(rid, globalid, multiplier, x1, y1, x2, y2, g)
with one as (
		select *, (st_dumppoints(g)).*  from livewire.primaryline 
    	),
    two as (
     select globalid, 
    st_makeline(geom, lead(geom) over (partition by globalid order by path))::geometry(LINESTRING,3448) g
    from one     
        )
    select  
    	row_number() over () rid, 
        globalid, 
        1::bigint multiplier ,
    	st_x(st_startpoint(g)) x1,
        st_y(st_startpoint(g)) y1, 
        st_x(st_endpoint(g)) x2,
        st_y(st_endpoint(g)) y2,
        g  
    from two where g is not null ; 
    
insert into nt.nodes2
    with one as (
select st_startpoint(g) p from nt.lines2
union
select st_endpoint(g) from nt.lines2)

select row_number() over () fid, 
  null,
  null::text status, 
  st_setsrid(p::geometry,3448)  g
from one ;

update nt.nodes2 set status = 'DEVICE' from livewire.isolators li where st_dwithin(li.g,nodes2.g,.005);
update nt.nodes2 set status = 'SOURCE' from livewire.isolators li where st_dwithin(li.g,nodes2.g,.005) and li.devicetype = 'FDR';
update nt.nodes2 set status = 'BLOCK' from livewire.isolators li where st_dwithin(li.g,nodes2.g,.005) and li.status = 'OPEN';

update nt.nodes2 set globalid =  facilityid::bigint from livewire.isolators li where st_dwithin(li.g,nodes2.g,.005);
update nt.nodes2 set globalid = facilityid::bigint, status = 'XFMR' from livewire.transformerbank li where st_dwithin(li.g,nodes2.g,.005);
update nt.lines2 set source = nodes2.rid from nt.nodes2 where st_dwithin(nodes2.g, st_startpoint(lines2.g),.005);
update nt.lines2 set target = nodes2.rid from nt.nodes2 where st_dwithin(nodes2.g, st_endpoint(lines2.g),.005);
update nt.lines2 set multiplier = 1e18 from nt.nodes2 where st_dwithin(nodes2.g, st_endpoint(lines2.g),.005) and status = 'BLOCK';

$BODY$;

ALTER FUNCTION nt.generate2()
    OWNER TO postgres;


/*****************/
-- FUNCTION: nt.lw_downstream_trace(integer)

-- DROP FUNCTION nt.lw_downstream_trace(integer);

CREATE OR REPLACE FUNCTION nt.lw_downstream_trace(
	device integer)
    RETURNS SETOF bigint 
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$

select distinct(
unnest(edges[(array_position(nodes::int[],device) ):]))
from nt.livewire where device =ANY(nodes)

$BODY$;

ALTER FUNCTION nt.lw_downstream_trace(integer)
    OWNER TO postgres;


/*******************/


-- FUNCTION: nt.lw_upstream_trace(integer)

-- DROP FUNCTION nt.lw_upstream_trace(integer);

CREATE OR REPLACE FUNCTION nt.lw_upstream_trace(
	device integer)
    RETURNS SETOF bigint 
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$

select distinct(
unnest(edges[:(array_position(nodes::int[],device)-1 )]))
from nt.livewire where device =ANY(nodes)

$BODY$;

ALTER FUNCTION nt.lw_upstream_trace(integer)
    OWNER TO postgres;
/**********************/


-- FUNCTION: nt.redirect2(bigint)

-- DROP FUNCTION nt.redirect2(bigint);

CREATE OR REPLACE FUNCTION nt.redirect2(
	source_id bigint)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

DECLARE
qq record;
jj record;
BEGIN

for qq in select distinct unnest(nodes[:cardinality(nodes) -1]) from nt.livewire where nodes[1] = source_id loop
 --raise notice '%', qq.unnest;
 select into jj lines.*,nodes.g ng from nt.lines, nt.nodes where st_intersects(nodes.g,lines.g) and nodes.fid = qq.unnest;
 if st_dwithin(jj.ng,st_endpoint(jj.g),.02) then
   raise notice 'FLIPPING LINE %', jj.fid;
   update nt.lines set g = st_reverse(g) where fid = jj.fid;
  END IF;
 END loop;

END;

$BODY$;

ALTER FUNCTION nt.redirect2(bigint)
    OWNER TO postgres;
/***************************/


-- FUNCTION: nt.redirect(bigint, bigint)

-- DROP FUNCTION nt.redirect(bigint, bigint);

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

ALTER FUNCTION nt.redirect(bigint, bigint)
    OWNER TO postgres;


/***************************/

-- FUNCTION: nt.retrace(bigint)

-- DROP FUNCTION nt.retrace(bigint);

CREATE OR REPLACE FUNCTION nt.retrace(
	source_id bigint)
    RETURNS void
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
AS $BODY$

delete from nt.ordered where transformer = source_id;
insert into nt.ordered
with list as (
 SELECT  *
  FROM pgr_dijkstra(
      'SELECT fid AS id, source::int AS source, target::int AS target, st_length(g) * multiplier AS cost FROM nt.lines ', 
      array[source_id]::BIGINT[],
      (select array_agg(fid) from 
		(select fid from nt.nodes where status = 'SOURCE' )as foo), 
      false)
 left join nt.nodes on node = fid 

 order by seq
    )
   
  
   
   select  
   	end_vid feeder, 
    start_vid transformer,  
    array_agg(edge order by path_seq) path, 
    array_agg(fid order by path_seq desc) filter (where status is not null) facility_ids from list
   group by 1,2
   having string_agg(status,',') !~ 'BLOCK'
  

$BODY$;

ALTER FUNCTION nt.retrace(bigint)
    OWNER TO postgres;



/*********************/

-- FUNCTION: nt.retrace_show(bigint)

-- DROP FUNCTION nt.retrace_show(bigint);

CREATE OR REPLACE FUNCTION nt.retrace_show(
	source_id bigint)
    RETURNS TABLE(transformer bigint, feeder bigint, path bigint, facility_ids bigint) 
    LANGUAGE 'sql'

    COST 100
    VOLATILE 
    ROWS 1000
AS $BODY$

with list as (
 SELECT  *
  FROM pgr_dijkstra(
      'SELECT fid AS id, source::int AS source, target::int AS target, st_length(g) AS cost FROM nt.lines ', 
      array[source_id]::BIGINT[],
      (select array_agg(fid) from 
		(select fid from nt.nodes where status = 'SOURCE' )as foo), 
      false)
 left join nt.nodes on node = fid 

 order by seq
    )
   
  
   
   select start_vid transformer, end_vid feeder,  edge path, fid facility_ids from list
   
  

$BODY$;

ALTER FUNCTION nt.retrace_show(bigint)
    OWNER TO postgres;
/***********************/
-- FUNCTION: nt.retraceall()

-- DROP FUNCTION nt.retraceall();

CREATE OR REPLACE FUNCTION nt.retraceall(
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $BODY$

  declare
    transformer bigint;
    cnt bigint;
  BEGIN
  cnt := 0;
  RAISE NOTICE 'Starting at %',clock_timestamp();
  for transformer in select fid from nt.nodes where status = 'XFMR' and fid not in (select ordered.transformer from nt.ordered) LOOP
  		-- RAISE NOTICE 'TRANSFORMER: %', transformer; 
  		perform nt.retrace(transformer);
        cnt := cnt + 1;
        if cnt in (100,1000,5000,10000,15000,30000) then
        	raise notice 'Reached % transformers at %',cnt, clock_timestamp();
        END IF;
  END LOOP;
  raise notice 'Finished at%', clock_timestamp();
  end;
  

$BODY$;

ALTER FUNCTION nt.retraceall()
    OWNER TO postgres;


