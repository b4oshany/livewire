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
 
 
 
 