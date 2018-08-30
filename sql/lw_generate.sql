/*		Gets the SRID of a livewire enabled schema 		*/

CREATE OR REPLACE FUNCTION lw_generate(
	  lw_schema text
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_generate$

DECLARE
  looprec record;
  srid int;
  
BEGIN
	
	srid = lw_getsrid(lw_schema);  -- GET LW_SRID
	EXECUTE format($$TRUNCATE %I.__lines$$,lw_schema);
	EXECUTE format($$TRUNCATE %I.__nodes$$,lw_schema);
	
	FOR looprec IN EXECUTE format('SELECT * from   %I.%I' , lw_schema,lw_schema) LOOP
		IF looprec.tabletype = 'EDGE' THEN
	  	PERFORM lw_generateedge(lw_schema,looprec.tablename);
	  ELSEIF looprec.tabletype = 'NODE' THEN
	  	PERFORM lw_generatenode(lw_schema,looprec.tablename);
	  END IF;
	END LOOP ;
	
	
	
	EXECUTE format($$ with one as ( 
		select st_astext(st_startpoint(g)) aa from %I.lines
		union
		select st_astext(st_endpoint(g)) from %I.lines
		),
		two as (select distinct aa from %I.nodes
		right join one on st_dwithin(g, st_setsrid(aa::geometry,%L),.001)
		where g is null)
	
		insert into %I.nodes (lw_table_pkid,status, g) SELECT -1, 'NODE', st_setsrid(aa::geometry,%L) from two
	
		$$,
	
		lw_schema,lw_schema,lw_schema, srid, lw_schema,srid);
	
	EXECUTE format('UPDATE %1$I.__lines l set source = n.lw_id from %1$I.__nodes n where st_equals(n.g, st_startpoint(l.g))',lw_schema);
	EXECUTE format('UPDATE %1$I.__lines l set target = n.lw_id from %1$I.__nodes n where st_equals(nodes.g, st_endpoint(l.g))',lw_schema);
	EXECUTE format($$UPDATE %1$I.__lines set multiplier = -1 from %1$I.__nodes where st_intersetcs(nodes.g,lines.g) and nodes.status = 'BLOCK'$$,lw_schema);
	

END;
$lw_generate$;
