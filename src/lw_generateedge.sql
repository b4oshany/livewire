
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