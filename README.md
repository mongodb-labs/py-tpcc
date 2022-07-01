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

