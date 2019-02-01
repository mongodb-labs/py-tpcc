# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
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

import logging
import time

class Results:

    def __init__(self):
        self.start = None
        self.stop = None
        self.txn_id = 0

        self.txn_counters = {}
        self.txn_times = {}
        self.txn_mins = {}
        self.txn_maxs = {}
        self.new_order_latencies = []
        self.txn_retries = {}
        self.running = {}

    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start is None, "start is not none on start"
        logging.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start

    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None, "start is none on stop"
        assert self.stop is None, "stop isn't none on stop"
        logging.debug("Stopping benchmark statistics collection")
        self.stop = time.time()

    def startTransaction(self, txn):
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time())
        return id

    def abortTransaction(self, id, retries=0):
        """Abort a transaction and discard its times"""
        assert id in self.running, "Didn't find self in running"
        txn_name, txn_start = self.running[id]
        del self.running[id]
        total_retries = self.txn_retries.get(txn_name, 0)
        self.txn_retries[txn_name] = total_retries + retries
        # txn_start is currently unused, which means we don't include aborted txn in timing metrics

    def stopTransaction(self, id, retries=0):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running, "Didn't find self in running"
        txn_name, txn_start = self.running[id]
        del self.running[id]

        duration = time.time() - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration
        if txn_name == 'NEW_ORDER':
            self.new_order_latencies.append(duration)

        total_retries = self.txn_retries.get(txn_name, 0)
        self.txn_retries[txn_name] = total_retries + retries

        total_cnt = self.txn_counters.get(txn_name, 0)
        self.txn_counters[txn_name] = total_cnt + 1

        min_time = self.txn_mins.get(txn_name, 10000000)
        if duration < min_time:
            self.txn_mins[txn_name] = duration

        max_time = self.txn_maxs.get(txn_name, 0)
        if duration > max_time:
            self.txn_maxs[txn_name] = duration

    def append(self, r):
        for txn_name in r.txn_counters.keys():
            orig_cnt = self.txn_counters.get(txn_name, 0)
            orig_min = self.txn_mins.get(txn_name, 10000000)
            orig_max = self.txn_maxs.get(txn_name, 0)
            orig_time = self.txn_times.get(txn_name, 0)
            orig_retries = self.txn_retries.get(txn_name, 0)

            self.txn_counters[txn_name] = orig_cnt + r.txn_counters[txn_name]
            self.txn_mins[txn_name] = orig_min if orig_min < r.txn_mins[txn_name] else r.txn_mins[txn_name]
            self.txn_maxs[txn_name] = orig_max if orig_max > r.txn_maxs[txn_name] else r.txn_maxs[txn_name]
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            self.txn_retries[txn_name] = orig_retries + r.txn_retries[txn_name]
            # logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
            if txn_name == 'NEW_ORDER':
                self.new_order_latencies.extend(r.new_order_latencies)

        ## HACK
        self.start = r.start
        self.stop = r.stop

    def __str__(self):
        return self.show()

    def show(self, load_time=None, driver=None, threads=1, warehouses=1):
        if not self.start:
            return "Benchmark not started"
        if not self.stop:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start

        col_width = 18
        num_columns = 11
        total_width = (col_width*num_columns)+2
        f = "\n  " + (("%-" + str(col_width) + "s")*num_columns)
        line = "-"*total_width

        ret = u"" + "="*total_width + "\n"
        if load_time:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)

        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Completed", u"DBTxnStarted", u"Time (µs)", u"Rate per thread", u"% Count", u"% Time", u"# Retries+aborts", u"min latency ms", u"avg latency ms", u"max latency ms")

        total_time = 0
        total_cnt = 0
        total_retries = 0
        total_dbtxn = 0
        for txn in self.txn_counters:
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            dbtxn_count = txn_cnt
            if txn == "DELIVERY":
                dbtxn_count = txn_cnt*10
            total_time += txn_time
            total_cnt += txn_cnt
            total_dbtxn += dbtxn_count

        for txn in sorted(self.txn_counters):
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            min_latency = u"%5.2f" % (1000 * self.txn_mins[txn])
            max_latency = u"%6.2f" % (1000 * self.txn_maxs[txn])
            dbtxn_count = txn_cnt
            if txn == "DELIVERY":
                dbtxn_count = txn_cnt*10
            txn_retries = self.txn_retries[txn]
            total_retries += txn_retries
            rate = u"%.02f txn/s" % ((txn_cnt / txn_time))
            avg_latency = u"%5.02f" % (1000* (txn_time / txn_cnt))
            perc_cnt = u"%5.02f" % ((100.0*txn_cnt / total_cnt))
            perc_time = u"%5.02f" % ((100.0*txn_time / total_time))
            ret += f % (txn, str(txn_cnt), str(dbtxn_count), str(txn_time), rate, perc_cnt, perc_time,
                        str(txn_retries)+"/"+str(100.00*txn_retries/dbtxn_count)[:5]+"%",
                        min_latency, avg_latency, max_latency)

        if 'NEW_ORDER' not in self.txn_counters:
            self.txn_counters['NEW_ORDER'] = 0
        ret += "\n" + ("-"*total_width)
        total_rate = "%.02f txn/s" % ((total_cnt / total_time))
        samples = len(self.new_order_latencies)
        ip50 = int(samples/2)
        ip75 = int(samples/100*75)
        ip90 = int(samples/100*90)
        ip95 = int(samples/100*95)
        ip99 = int(samples/100*99)
        lat = sorted(self.new_order_latencies)
        ret += f % ("TOTAL", str(total_cnt), str(total_dbtxn), str(total_time), total_rate, "", "", "", "", "", "")
        if driver != None:
            # print(driver)
            ret += "\n%s TpmC for %s %s thr %s txn %d WH: %d %d total %d durSec, batch %s %d retries %s%% %s fnM %s p50 %s p75 %s p90 %s p95 %s p99 %s max %s WC %s causal %s 10in1 %s retry %s %d" % (
                time.strftime("%Y-%m-%d %H:%M:%S"),
                ("normal", "denorm")[driver.denormalize],
                threads,
                ("with", "w/o ")[driver.no_transactions],
                warehouses,
                round(self.txn_counters['NEW_ORDER']*60/duration), self.txn_counters['NEW_ORDER'], duration,
                ("off", "on")[driver.batch_writes], total_retries, str(100.0*total_retries/total_dbtxn)[:5],
                ("w/o ", "with")[driver.find_and_modify],
                driver.read_preference,
                u"%6.2f" % (1000.0*lat[ip50]), u"%6.2f" % (1000.0*lat[ip75]),
                u"%6.2f" % (1000.0*lat[ip90]), u"%6.2f" % (1000.0*lat[ip95]), u"%6.2f" % (1000.0*lat[ip99]),
                u"%6.2f" % (1000.0*lat[-1]),
                str(driver.write_concern), ('false', 'true')[driver.causal_consistency],
                ('false', 'true')[driver.all_in_one_txn], ('false', 'true')[driver.retry_writes])
        return ret.encode('ascii', "ignore")
## CLASS
