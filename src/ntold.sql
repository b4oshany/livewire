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
