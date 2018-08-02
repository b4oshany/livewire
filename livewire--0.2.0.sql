/*

		LiveWire 0.1.0


*/

\echo Use "CREATE EXTENSION " to load this file. \quit\

CREATE TYPE lw_phase AS ENUM (
	'ABC',
	'AB',
	'AC',
	'BC',
	'A',
	'B',
	'C'
);

CREATE OR REPLACE FUNCTION lw_initialise(
	lw_schema text,
	lw_srid integer
	)
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_initialise$

DECLARE
  qry text;
  
BEGIN
execute format('CREATE SCHEMA %I',lw_schema);
execute format('CREATE TABLE %I.lines
                (
                    lw_table text,
                    lw_table_pkid NOT NULL,
                    lw_id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                    source integer,
                    target integer,
                    x1 double precision,
                    y1 double precision,
                    z1 double precision,
                    x2 double precision,
                    y2 double precision,
                    z2 double precision,
                    multiplier bigint,
                    phase lw_phase,
                    g geometry(LineStringZ,%L),
                    CONSTRAINT lines_pkey PRIMARY KEY (lw_id)
                )
                WITH (
                    OIDS = FALSE
                )',
        lw_schema,lw_srid);
execute format('CREATE TABLE %I.nodes
                (
                    lw_table text,
                    lw_table_pkid NOT NULL,
                    lw_id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
                    status text,
                    phase lw_phase,
                    g geometry(PointZ,3448),
                    CONSTRAINT nodes_pkey PRIMARY KEY (lw_id)
                )
                WITH (
                    OIDS = FALSE
                )',
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
                    tablename text,
                    tabletype text,
                    tableconfig json
                )
                WITH (
                    OIDS = FALSE
                )',
        lw_schema);

END;
$lw_initialise$;


CREATE OR REPLACE FUNCTION lw_addedgeparticipant(
    lw_schema text,
    participant text,
    phasemapping json,
    )
    RETURNS void
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE 
AS $lw_addedgeparticipant$

DECLARE
  qry text;
  holder record;
  geomcol text;
  
BEGIN
-- check that table exists
 SELECT pc.oid,pn.nspname,pc.relname, pc.relkind from pg_catalog.pg_class pc
 join pg_catalog.pg_namespace pn on pc.relnamespace=pn.oid
 where nspname = split_part(participant,'.',1)
 and relname = split_part(participant,'.',2) into holder;

 IF NOT FOUND THEN
    RAISE '% not found in system catalogs',participant;
 END IF;

--check that table has a geometry column   
SELECT attname from pg_catalog.pg_attribute pa
join pg_catalog.pg_type pt on pa.atttypid = pt.oid
WHERE attrelid = holder.oid AND pt.typname = 'geometry' into geomcol;

IF NOT FOUND THEN
    RAISE '% does not have a geometry column',participant;
 END IF;

--check that phase column exists
SELECT attname from pg_catalog.pg_attribute
where attname = phasemapping->>'columname' and attrelid = holder.oid;
IF NOT FOUND THEN
  RAISE 'phase column does not exist';
END IF;


END;
$lw_createlivewiretable$;