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
	EXECUTE format($$TRUNCATE %I.lines$$,lw_schema);
	EXECUTE format($$TRUNCATE %I.nodes$$,lw_schema);
	
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
	
	EXECUTE format('UPDATE %1$I.lines set source = nodes.lw_id from %1$I.nodes where st_dwithin(nodes.g, st_startpoint(lines.g),.001)',lw_schema);
	EXECUTE format('UPDATE %1$I.lines set target = nodes.lw_id from %1$I.nodes where st_dwithin(nodes.g, st_endpoint(lines.g), .001)',lw_schema);
	EXECUTE format($$UPDATE %1$I.lines set multiplier = -1 from %1$I.nodes where st_dwithin(nodes.g,lines.g,.001) and nodes.status = 'BLOCK'$$,lw_schema);
	

END;
$lw_generate$;
