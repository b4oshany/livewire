EXTENSION = livewire          # the extension name
DATA	  = $(wildcard *.sql)


# Postgres build stuff
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
