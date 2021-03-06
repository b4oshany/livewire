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