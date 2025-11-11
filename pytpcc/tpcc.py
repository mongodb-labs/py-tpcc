#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http:##www.cs.brown.edu/~pavlo/
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

import sys
import os
import string
import datetime
import logging
import re
import argparse
import glob
import time
import multiprocessing
import subprocess
import random
from configparser import ConfigParser
from pprint import pprint, pformat

from util import results, scaleparameters
from runtime import executor, loader

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    #
                    filename='results.log')

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(logging.Formatter(
    '%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s'))
logging.getLogger('').addHandler(console)

NOTIFY_PHASE_START_PATH = '/data/workdir/src/flamegraph/notify_phase_start.py'
NOTIFY_PHASE_END_PATH = '/data/workdir/src/flamegraph/notify_phase_end.py'

## ==============================================
## notifyDSIOfPhaseStart
## ==============================================
def notifyDSIOfPhaseStart(phasename):
    if os.path.isfile(NOTIFY_PHASE_START_PATH):
        output = subprocess.run(["python3", NOTIFY_PHASE_START_PATH, phasename], capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to notify DSI of phase starting:", output)
## DEF

## ==============================================
## notifyDSIOfPhaseEnd
## ==============================================
def notifyDSIOfPhaseEnd(phasename):
    if os.path.isfile(NOTIFY_PHASE_END_PATH):
        output = subprocess.run(["python3", NOTIFY_PHASE_END_PATH, phasename], capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to notify DSI of phase starting:", output)
## DEF

## ==============================================
## createDriverClass
## ==============================================
def createDriverClass(name):
    full_name = "%sDriver" % name.title()
    mod = __import__('drivers.%s' % full_name.lower(), globals(), locals(), [full_name])
    klass = getattr(mod, full_name)
    return klass
## DEF

## ==============================================
## getDrivers
## ==============================================
def getDrivers():
    drivers = []
    for f in [os.path.basename(drv).replace("driver.py", "") for drv in glob.glob("./drivers/*driver.py")]:
        if f != "abstract":
            drivers.append(f)
    return drivers
## DEF

## ==============================================
## startLoading. 
# This intentionally uses multiprocess pool and intentionally starts new processes for each batch
# because for long running, many hour long loads, the connection between the child process and the parent process is lost  
# and the parent process blocks indefinitelly waiting for the result.
## ==============================================
def startLoading(driverClass, scaleParameters, args, config):
    """
    Starts multiple worker processes to process warehouses in batches.  Each batch
    consists of 'clients' number of workers, each handling one warehouse.
    """
    clients = args['clients']
    logging.debug("Creating client pool with %d processes", clients)
    pool = multiprocessing.Pool(clients)

    # Calculate total number of warehouses
    total_warehouses = scaleParameters.ending_warehouse - scaleParameters.starting_warehouse + 1
    logging.debug(f"Total warehouses: {total_warehouses}")

    loader_results = []
    warehouse_ids = []
    block_size = total_warehouses // clients

    if(total_warehouses % clients != 0):
       logging.warning(f"WARNING: clients and warehouses are not well aligned {total_warehouses % clients} warehouses will be processed sequentially")

    ideal_ending_warehouse = scaleParameters.starting_warehouse + block_size * clients
    # create an array of warehouse IDs to 
    for i in range(block_size):
        for w_id in range(scaleParameters.starting_warehouse + i, ideal_ending_warehouse, block_size):
            logging.debug(f"adding warehouse {w_id} to warehouse_ids")
            warehouse_ids.append(w_id)
    # let's add all warehouses that are left
    for w_id in range(ideal_ending_warehouse, scaleParameters.ending_warehouse + 1):
        logging.debug(f"adding remaining warehouse {w_id} to warehouse_ids")
        warehouse_ids.append(w_id)
    assert len(warehouse_ids) == total_warehouses, "Mismatch in total warehouses and warehouse_ids length"

    # Shuffle warehouse IDs to distribute load across shards (not tested yet)
    # random.shuffle(warehouse_ids)
    # logging.info(f"Shuffled {len(warehouse_ids)} warehouses for parallel loading across shards")

    # Iterate through warehouses, processing them in batches of 'clients'
    for i in range(len(warehouse_ids)):
        w_id = warehouse_ids[i]
        logging.debug(f"Processing warehouse {w_id} in batch {i // clients}")

        # Apply the loader function asynchronously for the current warehouse
        r = pool.apply_async(loaderFunc, (driverClass, scaleParameters, args, config, [w_id]))
        loader_results.append(r)

        # If we've launched 'clients' workers, wait for them to complete before launching the next batch
        if (i + 1) % clients == 0:
            logging.debug(f"Waiting for batch {i // clients} to complete")
            for r in loader_results:
                try:
                    error_message = r.get()
                    if error_message:
                        logging.error(f"Worker process reported error: {error_message}")
                        raise RuntimeError(f"Failed to process batch: {error_message}")
                except Exception as e:
                    logging.error(f"Exception raised by worker process: {e}")
                    raise
            loader_results = []  # Clear results for next batch
            logging.debug(f"Starting batch {i // clients + 1}")
            time.sleep(5)

    # Wait for any remaining workers (in the last partial batch) to complete
    if loader_results:
        logging.debug("Waiting for the final batch to complete")
        for r in loader_results:
            try:
                error_message = r.get()
                if error_message:
                    logging.error(f"Worker process reported error: {error_message}")
                    raise RuntimeError(f"Failed to process final batch: {error_message}")
            except Exception as e:
                logging.error(f"Exception raised by worker process: {e}")
                raise

    pool.close()
    logging.debug("Waiting for all loaders to finish")
    pool.join()
    logging.info("All loading complete")
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids):
    # Add random delay (1-10 seconds) to prevent thundering herd when all clients connect simultaneously
    delay = random.uniform(1, 10)
    logging.debug("Client for warehouses %s: Delaying startup by %.2f seconds to stagger connections", w_ids, delay)
    time.sleep(delay)


    driver = driverClass(args['ddl'])
    assert driver != None, "Driver in loadFunc is none!"
    logging.debug("Starting client execution: %s [warehouses=%d]", driver, len(w_ids))

    config['load'] = True
    config['execute'] = False
    config['reset'] = False
    config['warehouses'] = args['warehouses']
    driver.loadConfig(config)

    try:
        loadItems = (1 in w_ids)
        l = loader.Loader(driver, scaleParameters, w_ids, loadItems)
        driver.loadStart()
        l.execute()
        driver.loadFinish()
    except KeyboardInterrupt:
        return -1
    except (Exception, AssertionError) as ex:
        logging.warn("Failed to load data: %s", ex)
        raise
    finally:
        # Ensure MongoDB client connection is properly closed
        if hasattr(driver, 'cleanup'):
            driver.cleanup()


## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes", args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)

    worker_results = []
    for _ in range(args['clients']):
        r = pool.apply_async(executorFunc, (driverClass, scaleParameters, args, config, debug,))
        worker_results.append(r)
    ## FOR
    pool.close()
    pool.join()

    total_results = results.Results()
    for asyncr in worker_results:
        asyncr.wait()
        r = asyncr.get()
        assert r != None, "No results object returned by thread!"
        if r == -1:
            sys.exit(1)
        total_results.append(r)
    ## FOR

    return total_results
## DEF

## ==============================================
## executorFunc
## ==============================================
def executorFunc(driverClass, scaleParameters, args, config, debug):
    driver = driverClass(args['ddl'])
    assert driver != None, "No driver in executorFunc"
    logging.debug("Starting client execution: %s", driver)

    config['execute'] = True
    config['load'] = False  # Explicitly set load to False for execution phase
    config['reset'] = False
    driver.loadConfig(config)

    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'], sameWH=args['samewh'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()
    # Ensure MongoDB client connection is properly closed
    if hasattr(driver, 'cleanup'):
        driver.cleanup()

    return results
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=str,
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
    aparser.add_argument('--samewh', default=85, type=float, metavar='PP',
                         help='Percent paying same warehouse')
    aparser.add_argument('--warehouses', default=4, type=int, metavar='W',
                         help='Number of Warehouses')
    aparser.add_argument('--duration', default=60, type=int, metavar='D',
                         help='How long to run the benchmark in seconds')
    aparser.add_argument('--ddl',
                         default=os.path.realpath(os.path.join(os.path.dirname(__file__), "tpcc.sql")),
                         help='Path to the TPC-C DDL SQL file')
    aparser.add_argument('--clients', default=1, type=int, metavar='N',
                         help='The number of blocking clients to fork')
    aparser.add_argument('--stop-on-error', action='store_true',
                         help='Stop the transaction execution when the driver throws an exception.')
    aparser.add_argument('--no-load', action='store_true',
                         help='Disable loading the data')
    aparser.add_argument('--no-execute', action='store_true',
                         help='Disable executing the workload')
    aparser.add_argument('--print-config', action='store_true',
                         help='Print out the default configuration file for the system and exit')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']:
        logging.getLogger().setLevel(logging.DEBUG)

    ## Create a handle to the target client driver
    driverClass = createDriverClass(args['system'])
    assert driverClass != None, "Failed to find '%s' class" % args['system']
    driver = driverClass(args['ddl'])
    assert driver != None, "Failed to create '%s' driver" % args['system']
    if args['print_config']:
        config = driver.makeDefaultConfig()
        print(driver.formatConfig(config))
        print()
        sys.exit(0)

    ## Load Configuration file
    configFilePath = args['config']
    if configFilePath:
        logging.debug("Loading configuration file '%s'", configFilePath)
        cparser = ConfigParser()
        cparser.read(os.path.realpath(configFilePath))
        config = dict(cparser.items(args['system']))
    else:
        logging.debug("Using default configuration for %s", args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict([(param, defaultConfig[param][1]) for param in defaultConfig.keys()])
    config['reset'] = args['reset']
    config['load'] = not args['no_load']    # True if loading, False if --no-load
    config['execute'] = args['no_load']     # True if --no-load (execution only), False if loading
    if config['reset']:
        logging.info("Reseting database")
    config['warehouses'] = args['warehouses']
    driver.loadConfig(config)
    logging.info("Initializing TPC-C benchmark using %s", driver)

    ## Create ScaleParameters
    scaleParameters = scaleparameters.makeWithScaleFactor(args['warehouses'], args['scalefactor'])
    if args['debug']:
        logging.debug("Scale Parameters:\n%s", scaleParameters)

    ## DATA LOADER!!!
    load_time = None
    if not args['no_load']:
        logging.info("Loading TPC-C benchmark data using %s", (driver))
        notifyDSIOfPhaseStart("TPC-C_load")
        load_start = time.time()
        if args['clients'] == 1:
            l = loader.Loader(
                driver,
                scaleParameters,
                range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1),
                True
            )
            driver.loadStart()
            l.execute()
            driver.loadFinish()
        else:
            startLoading(driverClass, scaleParameters, args, config)
        load_time = time.time() - load_start
        notifyDSIOfPhaseEnd("TPC-C_load")
    ## IF

    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        notifyDSIOfPhaseStart("TPC-C_workload")
        if args['clients'] == 1:
            e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'], sameWH=args['samewh'])
            driver.executeStart()
            results = e.execute(args['duration'])
            driver.executeFinish()
        else:
            results = startExecution(driverClass, scaleParameters, args, config)
        assert results, "No results from execution for %d client!" % args['clients']
        logging.info("Final Results")
        logging.info("Threads: %d", args['clients'])
        logging.info(results.show(load_time, driver, args['clients'], args['samewh']))
        notifyDSIOfPhaseEnd("TPC-C_workload")
    ## IF

## MAIN
