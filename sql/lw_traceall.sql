/*    Initiate trace of all sources   */

CREATE OR REPLACE FUNCTION lw_traceall(
  lw_schema text
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_traceall$

  declare
   
   looprec record;
   qrytxt text;
	 timer timestamptz;
	 starttime timestamptz;
   zerocount bigint;
  BEGIN
  starttime := clock_timestamp();


  /*    Verify that this source cannot reach other sources....that would be bad   */
  qrytxt := $_$
    select count(*) from pgr_dijkstra(
           $$select lw_id  id, source, target, st_length(g) * multiplier   as cost  
           from %1$I.lines  $$,
           (select lw_sourcenodes('%1$s')), 
           (select lw_sourcenodes('%1$s')), 
           false
           )
  $_$;
  EXECUTE format(qrytxt,lw_schema) into zerocount; 
  if zerocount > 0 THEN
    raise exception 'Zerocount is not zero!!';
  END IF;
 



  for looprec in EXECUTE(format($$select lw_id from %I.nodes where status = 'SOURCE'$$, lw_schema)) LOOP
  		RAISE NOTICE 'SOURCE: %', looprec.lw_id; 
		  timer := clock_timestamp();
  		perform lw_tracesource(lw_schema,looprec.lw_id::int, 50000, False);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;
  end;
  

$lw_traceall$;



CREATE OR REPLACE FUNCTION lw_traceall(
  lw_schema text,
  truncate boolean
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_traceall$

  declare
   
   looprec record;
	 timer timestamptz;
	 starttime timestamptz;
  BEGIN
  starttime := clock_timestamp();
  for looprec in EXECUTE(format($$
    select lw_id from %1$I.nodes where status = 'SOURCE'
    EXCEPT
    select distinct nodes[1] from %1$I.livewire$$, lw_schema)) LOOP
  		RAISE NOTICE 'SOURCE: %', looprec.lw_id; 
		  timer := clock_timestamp();
  		perform lw_tracesource(lw_schema,looprec.lw_id::int);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;
  end;
  

$lw_traceall$;
