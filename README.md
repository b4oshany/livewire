# Livewire: Sane power delivery modelling.


## What is LiveWire?

LiveWire is a postgresql extension that makes managing electrical distribution data painless. It provides wrapper functions around pgrouting to generate a pre cached routing network to make answering the most pertinent questions that electrical distribution engineers have.

## Requirements and Dependencies

- Postgresql 10
- Postgres PGXS extension
- Postgres Postgis extension

### Setup dependencies

#### Installing Postgres PGXS
```shell
sudo apt-get install postgresql-server-dev-all
sudo apt-get install postgresql-common
```

#### Installing Gdal and Postgis
```
sudo apt-get install postgresql-server-dev-all -y
sudo apt-get install postgresql-common -y
sudo apt-get install postgresql-10-postgis-2.4 -y
sudo apt-get install postgresql-10-postgis-scripts -y
sudo apt-get install postgis -y
sudo apt-get install postgresql-10-pgrouting -y
```

## Installation

Clone this repository.
Run:
``` shell
make && sudo make install
```

In the database that you want to enable LiveWire in run as a db superuser:

``` SQL
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_topology;
CREATE EXTENSION livewire;
```
## Usage

LiveWire groups common data together in a schema. For LiveWire to be effective the data must be connected. Functions and views are provided that will indicate suspect data.


```
lw_initialise(lw_name text, srid int)
```

This will make an existing schema ready for LiveWire by adding 2 support tables. If the schema does not exist it will create the scheam and add the tables. For an existing schema the following table names are not allowed:
-	__lines
-	__nodes
-	__livewire
-	$$schemaname$$
as these are the names of the support tables the LiveWire creates.

```
SELECT lw_initialise('powerflow',3448);
```

LiveWire imposes no restrictions on the structure of your data, you dont need to have certain column names existing for it to work. You do however have to configure it properly. Also, while it can manage three-phase electrical data, interrupting/isolating devices are at present either off for all phases or on for all phases.

```
lw_addedgeparticipant(lw_name text, lw_config json)
```

This will add a configuration directive for linear data (e.g. primary lines) The JSON blob must contain the following keys:

- schemaname - This is the name of the schema that the table is in. if schemaname is different from that of the livewire name (i.e. if when lw_initialise was run a new schema was created) then the data will be copied into the schema using CREATE TABLE LIKE semantics.

- tablename - this is the name of the table.

- primarykey - the column that is aprimary key or that you want to be a primary key.

- geomcolumn - the column with the geometry.

- labelcolumn - the column thatstores the name of the electrical source (usually the substation transformer ID)

- phasecolumn - the column that stores the phase data

- phasemap - a mapping of each of the possible 7 phase combinations to the data in the phasecolumn

```
lw_addnodeparticipant(lw_name text, lw_config json)
```

This will add a configuration directive for linear data (e.g. primary lines) The JSON blob must contain the following keys:

- schemaname - This is the name of the schema that the table is in. if schemaname is different from that of the livewire name (i.e. if when lw_initialise was run a new schema was created) then the data will be copied into the schema using CREATE TABLE LIKE semantics.

- tablename - this is the name of the table.

- primarykey - the column that is aprimary key or that you want to be a primary key.

- geomcolumn - the column with the geometry.

- labelcolumn - the column thatstores the name of the electrical source (usually the substation transformer ID)

- phasecolumn - the column that stores the phase data

- phasemap - a mapping of each of the possible 7 phase combinations to the data in the phasecolumn

- sourcequery - if this layer has the source (i.e. the substation transformer) then the where condition that satisfies a row to be a source. if the layer oly has source data, then specifying '1=1' would suffice. If the source is the substation transformer circuit breaker then 'devicetype=CB' might suffice.

- blockquery - If this layer is to have devices that can block the flow of current, then this would be the where conditin to identify them.

```
lw_generate(lw_schema text)
```
This will prepare the 'shadow' network. The shadow network is the combination of all the layers in a given livewire.

```
lw_traceall(lw_schema text)
```

This caches all possible routes. This operation is potential very expensive depending on number of sources and size of the network. It basically loops over all the sources and runs lw_tracesource().

