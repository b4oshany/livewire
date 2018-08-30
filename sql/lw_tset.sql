CREATE FUNCTION lw_tset()  RETURNS trigger AS $lw_tset$


  DECLARE


  BEGIN
    RAISE NOTICE '%', format('%I.%I',TG_TABLE_SCHEMA, TG_TABLE_NAME); 
    RAISE NOTICE '%', NEW.*;
    RAISE NOTICE '%', OLD.*;








  END;
$lw_tset$ LANGUAGE plpgsql;
