EXTENSION 		= livewire          # the extension name


DATA	  		= livewire--0.2.0.sql

release: 
	cat sql/*.sql > livewire--0.2.0.sql


# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
