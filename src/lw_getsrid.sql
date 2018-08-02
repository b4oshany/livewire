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
language sql