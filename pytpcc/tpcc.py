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
NOTIFY_PHASE_END_PATH = '/data/workdir/src/flamegraph/notify_phase_start.py'

## ==============================================
## noftifyDsiOfPhaseStart
## ==============================================
def noftifyDsiOfPhaseStart(phasename):
    if os.path.isfile(NOTIFY_PHASE_START_PATH):
        output = subprocess.run(["python3", NOTIFY_PHASE_START_PATH, phasename], capture_output=True)
        if output.returncode != 0:
            raise RuntimeError("Failed to notify DSI of phase starting:", output)
## DEF

## ==============================================
## noftifyDsiOfPhaseStart
## ==============================================
def noftifyDsiOfPhaseEnd(phasename):
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
## startLoading
## ==============================================
def startLoading(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes", args['clients'])
    pool = multiprocessing.Pool(args['clients'])

    # Split the warehouses into chunks
    w_ids = [[] for _ in range(args['clients'])]
    for w_id in range(scaleParameters.starting_warehouse, scaleParameters.ending_warehouse+1):
        idx = w_id % args['clients']
        w_ids[idx].append(w_id)
    ## FOR

    loader_results = []
    try:
        del args['config']
    except KeyError:
        print()
    for i in range(args['clients']):
        r = pool.apply_async(loaderFunc, (driverClass, scaleParameters, args, config, w_ids[i]))
        loader_results.append(r)
    ## FOR

    pool.close()
    logging.debug("Waiting for %d loaders to finish", args['clients'])
    pool.join()
## DEF

## ==============================================
## loaderFunc
## ==============================================
def loaderFunc(driverClass, scaleParameters, args, config, w_ids):
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

## DEF

## ==============================================
## startExecution
## ==============================================
def startExecution(driverClass, scaleParameters, args, config):
    logging.debug("Creating client pool with %d processes", args['clients'])
    pool = multiprocessing.Pool(args['clients'])
    debug = logging.getLogger().isEnabledFor(logging.DEBUG)
    try:
        del args['config']
    except KeyError:
        print()
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
    config['reset'] = False
    driver.loadConfig(config)

    e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
    driver.executeStart()
    results = e.execute(args['duration'])
    driver.executeFinish()

    return results
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Python implementation of the TPC-C Benchmark')
    aparser.add_argument('system', choices=getDrivers(),
                         help='Target system driver')
    aparser.add_argument('--config', type=open,
                         help='Path to driver configuration file')
    aparser.add_argument('--reset', action='store_true',
                         help='Instruct the driver to reset the contents of the database')
    aparser.add_argument('--scalefactor', default=1, type=float, metavar='SF',
                         help='Benchmark scale factor')
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
    if args['config']:
        logging.debug("Loading configuration file '%s'", args['config'])
        cparser = ConfigParser()
        cparser.read(os.path.realpath(args['config'].name))
        config = dict(cparser.items(args['system']))
    else:
        logging.debug("Using default configuration for %s", args['system'])
        defaultConfig = driver.makeDefaultConfig()
        config = dict([(param, defaultConfig[param][1]) for param in defaultConfig.keys()])
    config['reset'] = args['reset']
    config['load'] = False
    config['execute'] = False
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
        noftifyDsiOfPhaseStart("TPC-C_load")
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
        noftifyDsiOfPhaseEnd("TPC-C_load")
    ## IF

    ## WORKLOAD DRIVER!!!
    if not args['no_execute']:
        noftifyDsiOfPhaseStart("TPC-C_workload")
        if args['clients'] == 1:
            e = executor.Executor(driver, scaleParameters, stop_on_error=args['stop_on_error'])
            driver.executeStart()
            results = e.execute(args['duration'])
            driver.executeFinish()
        else:
            results = startExecution(driverClass, scaleParameters, args, config)
        assert results, "No results from execution for %d client!" % args['clients']
        logging.info("Final Results")
        logging.info("Threads: %d", args['clients'])
        logging.info(results.show(load_time, driver, args['clients']))
        noftifyDsiOfPhaseEnd("TPC-C_workload")
    ## IF

## MAIN
