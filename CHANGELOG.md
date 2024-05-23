# Versions

## Version 0.0.1

**Features**

* [x] Create a `Database` class used to manage different SQLite tables
* [x] Create a `Table` class which represents the different elements of an SQLite table
* [x] Create a `Query` class which is responsible for executing the sql queries
* [x] Create a `QuerySet` class lists the different elements returned from the database and is responsible for evaluating the underlying query
* [x] The default manager `DatabaseManager` lists all the functions for running operations on the database
* [x] Create a `Functions` class which a superclass for SQLite database functions: `Length`, `Count`, `Max`, `Min`, `Avg`, `MD5Hash`, `SHA1`, `SHA224`, `SHA256`, `Variance`, `StDev`, ``

**Bugfixes**

* [x] The middlewares do not load properly or do not load at all

**Performance**

* [x] Improve general speed performance: code simplification (Migration, Query functions)

**Dependency Updates**
