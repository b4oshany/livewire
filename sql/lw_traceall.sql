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


  /*    Verify all sources cannot reach each other.... that would be bad   */
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
 


  qrytxt := $$ SELECT row_number() over (), count(lw_id) over (), lw_id
		FROM %I.nodes where status = 'SOURCE'$$;
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
                RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count;
                timer := clock_timestamp();
                perform lw_redirect(lw_schema,looprec.lw_id::int);
                perform lw_tracesource(lw_schema, looprec.lw_id::int, 50000);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;




/*
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
  		RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count; 
		  timer := clock_timestamp();
  		perform lw_tracesource(lw_schema,looprec.lw_id::int, 50000, False);
		RAISE NOTICE '% | Elapsed time is %', clock_timestamp() - timer, clock_timestamp() - starttime;
  END LOOP;
  */
end;
  

$lw_traceall$;
