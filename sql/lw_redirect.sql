/*    'redirect' lines based upon their source origin    */

create or replace function lw_redirect(

    lw_schema text,
    source bigint,
    visitedl bigint[] default array[-1]::bigint[],
    visitedn bigint[] default array[-1]::bigint[]
  )
    RETURNS void
    LANGUAGE 'plpgsql'
    
    AS $lw_traceall$

  declare
   qrytxt text;
   updtxt text;
   looprec record;
   timer timestamptz;
  BEGIN
   /*    Trace from all blocks to source   */
  

  qrytxt := $$SELECT n.lw_id node_id, l.lw_id line_id, source, target, 
		--case when st_equals(n.g,st_startpoint(l.g)) then 'GOOD' ELSE 'FLIP' END stat
		case when st_dwithin(n.g,st_startpoint(l.g),.01) then 'GOOD' ELSE 'FLIP' END stat
		from %1$I.nodes n,%1$I.lines l
		where 
		--st_intersects(n.g,l.g)
		st_dwithin(n.g,l.g,.01)
		and n.lw_id = %2$s 
		and not (l.lw_id =ANY (%3$L))
		and not (n.lw_id =ANY (%4$L)) 
		and status <> 'BLOCK' $$;

  
  
  for looprec in EXECUTE(format(qrytxt,lw_schema,source,visitedl,visitedn)) LOOP
--  		RAISE NOTICE '%', looprec; 
	if looprec.stat = 'FLIP' THEN
	  updtxt := $$UPDATE %1$I.lines
                        set g = st_reverse(g),
                        source = %2$s,
                        target = %3$s
                        where lw_id = %4$s returning *$$;

	--raise notice '%', format(updtxt, lw_schema,looprec.target, looprec.source,looprec.line_id) ;
		
	execute  format(updtxt, lw_schema,looprec.target, looprec.source,looprec.line_id) ;
	visitedl := visitedl || looprec.line_id::bigint;		 
	visitedn := visitedn || looprec.target::bigint;
--	raise notice 'visitedl:  %', visitedl;
--	raise notice 'visitedn:  %', visitedn;
	source := looprec.source;
	else
        visitedl := visitedl || looprec.line_id::bigint;		 
        visitedn := visitedn || looprec.source::bigint;
	source := looprec.target;
	end if;

--	raise notice '%', format('SELECT lw_redirect_(%1$L,%2$s,%3$L,%4$L)',  
--		lw_schema,source,visitedl,visitedn);

	execute format('SELECT lw_redirect_(%1$L,%2$s,%3$L,%4$L)', 
			lw_schema,source,visitedl,visitedn);

END LOOP;
  end;
  

$lw_traceall$;
