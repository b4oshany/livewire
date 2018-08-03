/*    Gets the SRID of a livewire enabled schema    */

create or replace function lw_redirect(

    lw_schema text,
    source bigint,
    distance bigint default 17500
  )
    RETURNS void
    LANGUAGE 'plpgsql'
    
    AS $lw_traceall$

  declare
   qrytxt text;
   looprec record;
	timer timestamptz;
  BEGIN
   /*    Trace from all blocks to source   */
  qrytxt := $_$
		  select  
          *
        from  pgr_drivingDistance(
        	 $$select lw_id  id, source, target, st_length(g) as cost  
        	 from %1$I.lines
        	 where lines.g && st_expand(
        	  (select nodes.g from %1$I.nodes where nodes.lw_id = %2$s)
        	  ,%3$s)
        	 $$,
        	 %2$s,
        	 %3$s,
        	 false
        	 )
        
  $_$;
  
  for looprec in EXECUTE(format(qrytxt,lw_schema,source,distance)) LOOP
  		RAISE NOTICE '%', looprec; 
		  
  END LOOP;
  end;
  

$lw_traceall$;
