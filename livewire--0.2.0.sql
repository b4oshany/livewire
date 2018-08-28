/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_addedgeparticipant(
    lw_schema text,
    edgeinfo json
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_addedgeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
 
BEGIN
EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'EDGE', %4$L)$$,
    lw_schema, edgeinfo->>'schemaname',edgeinfo->>'tablename', edgeinfo
);
END;
$lw_addedgeparticipant$;/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_addnodeparticipant(
    lw_schema text,
    nodeinfo json
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_addnodeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
 
BEGIN
EXECUTE format(
    $$INSERT INTO %1$I.%1$I (tablename, tabletype, tableconfig) 
    VALUES ('%2$I.%3$I', 'NODE', %4$L)$$,
    lw_schema, nodeinfo->>'schemaname',nodeinfo->>'tablename', nodeinfo
);
END;
$lw_addnodeparticipant$;/*    Returns an array of all nodes that block     */

CREATE OR REPLACE FUNCTION lw_blocknodes(
    in lw_schema text,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_blocknodes$

DECLARE
  qrytxt text;

BEGIN
  -- Find all block nodes in a given livewire
  
  qrytxt := $$select array_agg(lw_id) from %1$I.nodes where status = 'BLOCK'$$;
  
  execute format(qrytxt,lw_schema) into myarray;
END;
 $lw_blocknodes$;


CREATE OR REPLACE FUNCTION lw_blocknodes(
    in lw_schema text,
    in source bigint,
    in distance bigint,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'
AS
 $lw_blocknodes$

DECLARE
  qrytxt text;

BEGIN
  -- Find all end nodes near a given node
  qrytxt := $$select array_agg(lw_id) from %1$I.nodes 
              WHERE nodes.g && st_expand(
                (select g from %1$I.nodes where lw_id = %2$s), %3$s)
              AND status = 'BLOCK'$$;
  execute format(qrytxt,lw_schema, source, distance) into myarray;
END;
 $lw_blocknodes$;
 
 
 
 
/*    Returns an array of lw_ids that correspond to endnodes    */

CREATE OR REPLACE FUNCTION lw_endnodes(
    in lw_schema text,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_endnodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := 'select array_agg(lw_id) from
		(select source lw_id from 
		(select lw_id, source from %1$I.lines 
		union 
		select lw_id, target from %1$I.lines ) as lines
		group by source  
		having count(lw_id) = 1) as lw_ids';
  
  execute format(qrytxt,lw_schema) into myarray;
END;
 $lw_endnodes$;


CREATE OR REPLACE FUNCTION lw_endnodes(
    in lw_schema text,
    in source bigint,
    in distance bigint,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'
AS
 $lw_endnodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes near a given node
  
  qrytxt := 'with lines as (
		select * from %1$I.lines 
		where lines.g && (
		select st_expand(nodes.g,%3$L) from %1$I.nodes where nodes.lw_id = %2$L)
			)
		
		select array_agg(lw_id)::bigint[] from (
		select source lw_id from (
		select lw_id, source from lines
		union
		select lw_id, target from lines
			) as foo group by source having count(lw_id) = 1
			) as lw_ids';
  execute format(qrytxt,lw_schema, source, distance) into myarray;
END;
 $lw_endnodes$;
 
 
 
 
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
$lw_generate$;/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_generateedge(
    lw_schema text,
    tablename text
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_addedgeparticipant$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  ei json;
BEGIN
  srid = lw_getsrid(lw_schema);  -- GET LW_SRID
  
  /*    Get table config data   */
  EXECUTE format(
    'select tableconfig from %1$I.%1$I where tablename = %2$L',
    lw_schema,tablename
  ) into ei; 

  /*    check that table exists   */
  PERFORM * from pg_catalog.pg_class pc
    JOIN pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
    where nspname = ei->>'schemaname'
    and relname = ei->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ei->>'tablename';
  END IF;

  /*    check that table has a geometry column    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class on attrelid = oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname' and relname = ei->>'tablename' 
    and attname = ei->>'geomcolumn' and typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ei->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ei->>'schemaname'
    and relname = ei->>'tablename' and attname = ei->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    Check that config info has the correct phase keys   */
  PERFORM count(*) from json_each_text(ei->'phasemap')
    where key in ('ABC','AB','AC','BC','A','B','C') and value is not null
    except select 7;
  IF FOUND THEN
    RAISE 'phase column mapping not accurate';
  END IF;

  /*    Check that unique column is unique    */
  EXECUTE format(
    'SELECT %3$I from %1$I.%2$I group by %3$I having count(%3$I) > 1',
    ei->>'schemaname', ei->>'tablename', ei->>'primarykey'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Primary key column is not unique';
  END IF;

  /*    Check that geometry column has no duplicates    */
  EXECUTE format(
    'SELECT st_astext(%3$I) from %1$I.%2$I group by %3$I having count(st_astext(%3$I)) > 1',
    ei->>'schemaname', ei->>'tablename', ei->>'geomcolumn'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Geometry column has duplicate points; Table %', tablename ;
  END IF;
  
  qrytxt := format($qrytxt$ 
    with one as (
      select 
        %3$I pk, 
        CASE 
          WHEN %5$I = %6$L THEN 'ABC'
          WHEN %5$I = %7$L THEN 'AB'
          WHEN %5$I = %8$L THEN 'BC'
          WHEN %5$I = %9$L THEN 'AC'
          WHEN %5$I = %10$L THEN 'A'
          WHEN %5$I = %11$L THEN 'B'
          WHEN %5$I = %12$L THEN 'C'
        END phase,
        (st_dumppoints(%4$I)).* 
      from %1$I.%2$I
    ),
    two as (
      select 
        pk, phase, st_force3d(st_setsrid(st_makeline(geom,lead(geom) 
        over (partition by pk order by path)),%13$L))::geometry(LINESTRINGZ,%13$L) geom 
     from one)
    select 
      '%1$I.%2$I' lw_table, pk lw_table_pkid, st_x(st_startpoint(geom)) x1, 
      st_y(st_startpoint(geom)) y1, st_z(st_startpoint(geom)) z1, 
      st_x(st_endpoint(geom)) x2, st_y(st_endpoint(geom)) y2, 
      st_z(st_endpoint(geom)) z2, 1 multiplier, phase, geom
    from two 
    where 
     geom is not null
    $qrytxt$,
    ei->>'schemaname', ei->>'tablename', ei->>'primarykey', ei->>'geomcolumn',
    ei->>'phasecolumn', ei->'phasemap'->>'ABC', ei->'phasemap'->>'AB',
    ei->'phasemap'->>'BC', ei->'phasemap'->>'AC', ei->'phasemap'->>'A',
    ei->'phasemap'->>'B', ei->'phasemap'->>'C', srid);
  

EXECUTE format(
  'INSERT INTO %I.lines (lw_table, lw_table_pkid,x1,y1,z1,x2,y2,z2,multiplier,phase,g) %s',
  lw_schema,qrytxt
  );

END;
$lw_addedgeparticipant$;
/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_generatenode(
    lw_schema text,
    tablename text
  )
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_generatenode$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  ni json;
BEGIN
  srid = lw_getsrid(lw_schema);  -- GET LW_SRID
  
  /*    Get table config data   */
  EXECUTE format(
    'select tableconfig from %1$I.%1$I where tablename = %2$L',
    lw_schema,tablename
  ) into ni; 
  
  /*    check that table exists   */
  PERFORM * from pg_catalog.pg_class pc
    JOIN pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
    where nspname = ni->>'schemaname'
    and relname = ni->>'tablename';
  IF NOT FOUND THEN
    RAISE '% not found in system catalogs', ni->>'tablename';
  END IF;
  
  /*    check that table has a geometry column    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class on attrelid = oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname' and relname = ni->>'tablename' 
    and attname = ni->>'geomcolumn' and typname = 'geometry';
  IF NOT FOUND THEN
    RAISE '% not found or is not of type geometry', ni->>'geomcolumn';
  END IF;

  /*    check that phase column exists    */
  PERFORM * from pg_catalog.pg_attribute pa
    JOIN pg_catalog.pg_type pt on pa.atttypid = pt.oid
    JOIN pg_catalog.pg_class pc on attrelid = pc.oid
    JOIN pg_catalog.pg_namespace pn on relnamespace = pn.oid
    WHERE nspname = ni->>'schemaname'
    and relname = ni->>'tablename' and attname = ni->>'phasecolumn';
  IF NOT FOUND THEN
    RAISE 'phase column does not exist';
  END IF;

  /*    Check that config info has the correct phase keys   */
  PERFORM count(*) from json_each_text(ni->'phasemap')
    where key in ('ABC','AB','AC','BC','A','B','C') and value is not null
    except select 7;
  IF FOUND THEN
    RAISE 'phase column mapping not accurate';
  END IF;

  /*    Check that unique column is unique    */
  EXECUTE format(
    'SELECT %3$I from %1$I.%2$I group by %3$I having count(%3$I) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'primarykey'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Primary key column is not unique';
  END IF;

  /*    Check that geometry column has no duplicates    */
  EXECUTE format(
    'SELECT st_astext(%3$I) from %1$I.%2$I group by %3$I having count(st_astext(%3$I)) > 1',
    ni->>'schemaname', ni->>'tablename', ni->>'geomcolumn'
  );
  GET DIAGNOSTICS diaginfo = ROW_COUNT;
  IF diaginfo > 0 THEN
    RAISE 'Geometry column has duplicate points';
  END IF;

  qrytxt := format($qrytxt$ 
    with one as (
      select 
        %3$I pk, 
        CASE 
          WHEN %5$I = %6$L THEN 'ABC'
          WHEN %5$I = %7$L THEN 'AB'
          WHEN %5$I = %8$L THEN 'BC'
          WHEN %5$I = %9$L THEN 'AC'
          WHEN %5$I = %10$L THEN 'A'
          WHEN %5$I = %11$L THEN 'B'
          WHEN %5$I = %12$L THEN 'C'
        END::text phase,
        CASE  
          WHEN %14$s THEN 'SOURCE'
          WHEN %15$s THEN 'BLOCK'
          ELSE 'DEVICE' 
        END status,
        %4$I geom 
      from %1$I.%2$I
   ),
   two as (
     select 
       pk, 
       phase, 
       status,
       st_force3d(st_setsrid(geom,%13$L))::geometry(POINTZ,%13$L) geom 
    from one)
  select 
    '%1$I.%2$I' lw_table, pk lw_table_pkid, status, phase, geom from two 
  where geom is not null
  $qrytxt$,
  ni->>'schemaname', ni->>'tablename', ni->>'primarykey', ni->>'geomcolumn',
  ni->>'phasecolumn', ni->'phasemap'->>'ABC', ni->'phasemap'->>'AB', 
  ni->'phasemap'->>'BC', ni->'phasemap'->>'AC', ni->'phasemap'->>'A', 
  ni->'phasemap'->>'B', ni->'phasemap'->>'C', srid, ni->>'sourcequery',
  ni->>'blockquery');
   
  EXECUTE format('INSERT INTO %I.nodes (lw_table, lw_table_pkid, status, phase,g) %s',
  lw_schema, qrytxt
  );

END;
$lw_generatenode$;/*		Gets the SRID of a livewire enabled schema 		*/

create or replace function lw_getsrid(in lw_schema text) returns bigint as 
$$
select regexp_replace(geometry_typmod_out(atttypmod)::text,'[A-z,()]','','g')::bigint from pg_catalog.pg_attribute pa
join pg_catalog.pg_type pt on pa.atttypid = pt.oid
join pg_catalog.pg_class on attrelid = oid
join pg_catalog.pg_namespace pn on relnamespace = pn.oid
WHERE nspname = lw_schema and 
relname = 'nodes' 
and attname = 'g' and typname = 'geometry' ;	
$$
language sql;
/*      Creates schema and livewire base tables      */

CREATE OR REPLACE FUNCTION lw_initialise(
	lw_schema text,
	lw_srid integer
	)
    RETURNS void
    LANGUAGE 'plpgsql'

AS $lw_initialise$

DECLARE
  qry text;
  
BEGIN
execute format('CREATE SCHEMA %I',lw_schema);
execute format($$CREATE TABLE %I.lines
                (
                    lw_table text,
                    lw_table_pkid text NOT NULL,
                    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    source integer,
                    target integer,
                    x1 double precision,
                    y1 double precision,
                    z1 double precision,
                    x2 double precision,
                    y2 double precision,
                    z2 double precision,
                    multiplier bigint,
                    phase text,
                    g geometry(LineStringZ,%L),
                    CONSTRAINT phase_check CHECK (phase = ANY (ARRAY['ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text, 'A'::text, 'B'::text, 'C'::text]))
                )
                WITH (
                    OIDS = FALSE
                )$$,
        lw_schema,lw_srid);
execute format($$CREATE TABLE %I.nodes
                (
                    lw_table text,
                    lw_table_pkid text NOT NULL,
                    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    status text,
                    phase text,
                    g geometry(PointZ,%L),
                    CONSTRAINT phase_check CHECK (phase = ANY (ARRAY['ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text, 'A'::text, 'B'::text, 'C'::text]))
                )
                WITH (
                    OIDS = FALSE
                )$$,
        lw_schema,lw_srid);
execute format('CREATE TABLE %I.livewire
                (
                    nodes bigint[],
                    edges bigint[]
                )
                WITH (
                    OIDS = FALSE
                )',
        lw_schema);
execute format('CREATE TABLE %I.%I
                (
                    tablename text PRIMARY KEY,
                    tabletype text,
                    tableconfig json
                )
                WITH (
                    OIDS = FALSE
                )',
        lw_schema,lw_schema);
execute format('CREATE INDEX ON %I.lines USING gist (g)',lw_schema);
execute format('CREATE INDEX ON %I.nodes USING gist (g)',lw_schema);

END;
$lw_initialise$;
/*    Gets the SRID of a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_nodesnearnode(
    in lw_schema text,
    in source bigint,
    in distance bigint,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql' AS 

$lw_nodesnearnode$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes near a given node
  qrytxt := 'select array_agg(lw_id)::bigint[] from (
              select nodes.lw_id from %1$I.nodes
              join %1$I.lines on st_dwithin(nodes.g,lines.g,.001)
              where lines.g && (
                select st_expand(nodes.g,%3$L)
                from %I.nodes where nodes.lw_id = %2$L
                )
              group by nodes.lw_id
              having count(lines.lw_id) = 1) 
            as foo)';
  execute(qrytxt,lw_schema, source, distance) into myarray;

END;
$lw_nodesnearnode$;/*    'redirect' lines based upon their source origin    */

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
/*    Returns an array of all SOURCE nodes in a livewire enabled schema    */

CREATE OR REPLACE FUNCTION lw_sourcenodes(
    in lw_schema text,
    out myarray bigint[]
  )
    LANGUAGE 'plpgsql'

AS $lw_sourcenodes$

DECLARE
  diaginfo bigint;
  qrytxt text;
  srid int;
  nodeinfo json;
 
BEGIN
  -- Find all end nodes in a given livewire
  
  qrytxt := $$select array_agg(lw_id) from %1$I.nodes
		where status = 'SOURCE'$$;
  
  execute format(qrytxt,lw_schema) into myarray;
END;
 $lw_sourcenodes$;/*    Initiate trace of all sources   */

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
 


  qrytxt := $$ SELECT row_number() over (), count(lw_id) over (), lw_id
		FROM %I.nodes where status = 'SOURCE'$$;
  for looprec in EXECUTE(format(qrytxt, lw_schema)) LOOP
                RAISE NOTICE 'SOURCE: % | % of %', looprec.lw_id,looprec.row_number, looprec.count;
                  timer := clock_timestamp();
                perform lw_redirect(lw_schema,looprec.lw_id::int);
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
/*      Gets the SRID of a livewire enabled schema      */

CREATE OR REPLACE FUNCTION lw_tracednstream(
    in lw_schema text,
	in lw_id bigint,
	out g geometry)

    LANGUAGE 'plpgsql'

AS $lw_tracednstream$

DECLARE
    qrytxt text;


BEGIN
    qrytxt := 'select st_union(g) g from %1$I.lines where lw_id in (select distinct(
                unnest(edges[(array_position(nodes::int[], %2$s)):]))
                from %1$I.livewire where %2$s =ANY(nodes))';
    
    
    execute format(qrytxt, lw_schema, lw_id) into g;
END;
$lw_tracednstream$;


CREATE OR REPLACE FUNCTION lw_tracednstream(
    in lw_schema text,
	in lw_tablename text,
	in lw_table_pkid text,
	out g geometry)

    LANGUAGE 'plpgsql'

AS $lw_tracednstream$

DECLARE
    lw_id bigint;
    qrytxt text;


BEGIN
    qrytxt := 'select lw_id from %1$I.nodes 
                where lw_table = %2$L 
                and lw_table_pkid = %3$L';
    
    raise notice '%',  format(qrytxt, lw_schema, lw_tablename, lw_table_pkid);
    execute format(qrytxt, lw_schema, lw_tablename, lw_table_pkid) into lw_id;
    if lw_id is null then
		g := null::geometry;
	else
    	g = lw_tracednstream(lw_schema,lw_id);
	END IF;
    
END;
$lw_tracednstream$;/*    Gets the SRID of a livewire enabled schema    */

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
 /*
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
*/




END;
$lw_tracesource$;
/*      Gets the SRID of a livewire enabled schema      */

CREATE OR REPLACE FUNCTION lw_traceupstream(
    in lw_schema text,
	in lw_id bigint,
	out g geometry)

    LANGUAGE 'plpgsql'

AS $lw_traceupstream$

DECLARE
    qrytxt text;


BEGIN
    qrytxt := 'select st_union(g) g from %1$I.lines where lw_id in (select distinct(
                unnest(edges[:(array_position(nodes::int[], %2$s)-1 )]))
                from %1$I.livewire where %2$s =ANY(nodes))';
    
    
    execute format(qrytxt, lw_schema, lw_id) into g;
END;
$lw_traceupstream$;


CREATE OR REPLACE FUNCTION lw_traceupstream(
    in lw_schema text,
	in lw_tablename text,
	in lw_table_pkid text,
	out g geometry)

    LANGUAGE 'plpgsql'

AS $lw_traceupstream$

DECLARE
    lw_id bigint;
    qrytxt text;


BEGIN
    qrytxt := 'select lw_id from %1$I.nodes 
                where lw_table = %2$L 
                and lw_table_pkid = %3$L';
    
    raise notice '%',  format(qrytxt, lw_schema, lw_tablename, lw_table_pkid);
    execute format(qrytxt, lw_schema, lw_tablename, lw_table_pkid) into lw_id;
    if lw_id is null then
		g := null::geometry;
	else
    	g = lw_traceupstream(lw_schema,lw_id);
	END IF;
    
END;
$lw_traceupstream$;