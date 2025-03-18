## TPC-C in Python for MongoDB
Approved in July of 1992, TPC Benchmark C is an on-line transaction processing (OLTP) benchmark. TPC-C is more complex than previous OLTP benchmarks such as TPC-A because of its multiple transaction types, more complex database and overall execution structure. TPC-C involves a mix of five concurrent transactions of different types and complexity either executed on-line or queued for deferred execution. The database is comprised of nine types of tables with a wide range of record and population sizes. TPC-C is measured in transactions per minute (tpmC). While the benchmark portrays the activity of a wholesale supplier, TPC-C is not limited to the activity of any particular business segment, but, rather represents any industry that must manage, sell, or distribute a product or service.

To learn more about TPC-C, please see the [TPC-C](https://www.tpc.org/tpcc/) documentation.

This repo is an experimental variant of Python TPC-C implementation based on the original [here](http://github.com/apavlo/py-tpcc).

The structure of the repo is:

1. **pytpcc** - the code for pytpcc with driver (DB) specific code in **drivers** subdirectory.
2. **vldb2019** - 2019 VLDB paper, poster and results generated from this code
   * [VLDB Paper](vldb2019/paper.pdf)
   * [VLDB Poster](vldb2019/poster.pdf)
   * [Result directory](vldb2019/results)

All the tests were run using [MongoDB Atlas](https://www.mongodb.com/cloud/atlas?jmp=VLDB2019).
Use code `VLDB2019` to get $150 credit to get started with MongoDB Atlas.


## Postgres JSONB Driver

This branch contains a Postgres JSONB Driver.

Steps to run the PostgreSQL JSONB Driver

1. Start Postgres.

```bash
sudo systemctl start postgresql
```

2. Create ana activate a python env.

```bash
mkdir ~/python_envs
cd ~/python_envs
~/python_envs$ python -m venv py-tpcc-env
source ~/python_envs/py-tpcc-env/bin/activate
```

3. Print your config.

```bash
cd ~/py-tpcc/pytpcc
~/py-tpcc/pytpcc$ python ./tpcc.py --print-config postgresqljsonb > postgresqljsonb.config
```

3. Edit the configuraiton for Postgres in the postgresqljsonb.config. Add a password.

```bash
# PostgresqljsonbDriver Configuration File
# Created 2025-03-18 23:00:45.340852
[postgresqljsonb]

# The name of the PostgreSQL database
database             = tpcc

# The host address of the PostgreSQL server
host                 = localhost

# The port number of the PostgreSQL server
port                 = 5432

# The username to connect to the PostgreSQL database
user                 = postgres

# The password to connect to the PostgreSQL database
password             = <ADD_PASSWORD_HERE>
```

4. Run the PostgreSQL JSONB driver tests with resetting the database.

```bash
~/py-tpcc/pytpcc$ python ./tpcc.py --reset --clients=1 --duration=1 --warehouses=1 --ddl tpcc_jsonb.sql --config=postgresqljsonb.config postgresqljsonb --stop-on-error
```

5. Run the PostgreSQL JSONB driver tests with no load phase to use the data that is already loaded in the Postgres db.

```bash
~/py-tpcc/pytpcc$ python ./tpcc.py --no-load --clients=1 --duration=1 --warehouses=1 --ddl tpcc_jsonb.sql --config=postgresqljsonb.config postgresqljsonb --stop-on-error
```

6. If you need to connect to Postgres and check the database size

```bash
psql -U postgres # and type the password
postgres=\# \l+

# For any SQL command first use the database
use tpcc
```