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
-- execute format('CREATE SCHEMA %I',lw_schema);
execute format($$CREATE TABLE %I.__lines
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
                    feederid text,
                    g geometry(LineStringZ,%L),
                    CONSTRAINT phase_check CHECK (phase = ANY (ARRAY['ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text, 'A'::text, 'B'::text, 'C'::text]))
                )
                WITH (
                    OIDS = FALSE
                )$$,
        lw_schema,lw_srid);
execute format($$CREATE TABLE %I.__nodes
                (
                    lw_table text,
                    lw_table_pkid text NOT NULL,
                    lw_id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    status text,
                    phase text,
		    feederid text,
                    g geometry(PointZ,%L),
                    CONSTRAINT phase_check CHECK (phase = ANY (ARRAY['ABC'::text, 'AB'::text, 'AC'::text, 'BC'::text, 'A'::text, 'B'::text, 'C'::text]))
                )
                WITH (
                    OIDS = FALSE
                )$$,
        lw_schema,lw_srid);
execute format('CREATE TABLE %I.__livewire
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
execute format('CREATE INDEX ON %I.__lines USING gist (g)',lw_schema);
execute format('CREATE INDEX ON %I.__nodes USING gist (g)',lw_schema);

END;
$lw_initialise$;
