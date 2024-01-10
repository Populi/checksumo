# Overview

`checksumo` is a command-line tool for checking replication status between database replication pairs.
It works by generating checksums for Table "chunks" and comparing them between master and replica instances.
Any chunks that are observed to differ are then examined row-by-row to isolate the differing rows.

# Requirements

`checksumo` is written and tested on Ruby 3.2.2_1 and requires the following Ruby gems:

```
# to run
gem install 'logging'
gem install 'memoist3'
gem install 'mysql2'

# to execute tests
gem install 'rspec'
gem install 'factory_bot'
gem install 'simplecov'
gem install 'yaml'

```

This version is written and tested on MySQL 5.7.43.

# FAQ

### What is the performance impact of Table- or Row-Locking with `checksumo`?

`checksumo` runs in a `READ ONLY` session, with `ISOLATION LEVEL` of `READ COMMITTED`. This is the _least_ ACID-compliant SQL setting we have been able to find for MySQL.
Our intent is to reduce row- and table-locking as much as possible, although we are unable to remove transactional behavior entirely from MySQL interactions.
In our testing, we have observed no indicators of loss of performance from `checksumo`.

It's important to note that `checksumo` monitors a database replication pair and generates SQL commands to repair replication errors it finds, but it doesn't actually attempt repairs.

### How accurate is the replication error detection?

The goal of `checksumo` is to detect replication drift _when the database replication engine believes the instances to be in sync_.

`checksumo` first compares "chunks" (ordered collections of database table rows) for checksum, start and end rows, and row counts. If those are do not match, `checksumo` dives into each chunk, comparing each row between the master and replica servers.

`checksumo` finds rows in the master instance that aren't in the replica (missing replica rows), rows in the replica that aren't in the master (failed DELETE replication), and rows that are in both, but have differing values.

The program will attempt to generate appropriate SQL statements to be run on the replica in order to bring it back into sync with the master.

The default behavior is for `checksumo` to continuously monitor the two databases in order to see whether they come into sync within a "reasonable" time.

### How are checksums calculated?

`checksumo` uses the built-in `CRC32` SQL function to calculate checksums.

### How does lag time affect the replication checks?

`checksumo` only watches tables and/or rows that are not known to be in sync: as soon as a table has been observed to be in sync, `checksumo` ceases to pay attention to it. As a result, most tables are ignored very early in the process. This reduces lag, as very few tables and/or rows are actively under scrutiny.

Additionally, the program will re-examine tables and/or rows once they have been identified as "problem" spots. This is to allow the replication process to catch up before `checksumo` decides they're out of sync.

## What does the `checksumo.yml` file do?

The YAML file overrides the hard-coded default values for `checksumo`'s command-line options.
`checksumo` command-line invocations can be pretty verbose, and it seems like a good option for users to configure their own defaults.

# Known Issues

### Row- and Table-locking

`checksumo` runs in the least ACID-compliant session we can configure: `READ ONLY` access with `READ COMMITTED` isolation level. This is to reduce table and row locks as much as possible, but it's not possible to run the MySQL client entirely without ACID compliance.This means that tables and/or rows may be locked briefly during runtime.

While we haven't observed performance issues related to locking, we're still looking options to run entirely outside database transactions.

### Replication Lag

The program doesn't checks replication partners serially: first the master, then the replica. This is for two reasons: first, we need to reference values from the master in the SQL queries for the replica. Second, replication is necessarily laggy, and the reduced lag we observed in testing a concurrent solution didn't produce any measurable improvements.
