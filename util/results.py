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
from collections import Counter

class Results:

    def __init__(self):
        self.start = None
        self.stop = None
        self.txn_id = 0

        self.txn_counters = {}
        self.txn_times = {}
        self.txn_mins = {}
        self.txn_maxs = {}
        self.latencies = {}
        self.txn_retries = {}
        self.txn_aborts = {}
        self.retries = {}
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
        total_aborts = self.txn_aborts.get(txn_name, 0)
        self.txn_aborts[txn_name] = total_aborts + 1
        # txn_start is currently unused, which means we don't include aborted txn in timing metrics

    def stopTransaction(self, id, retries=0):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running, "Didn't find self in running"
        txn_name, txn_start = self.running[id]
        del self.running[id]

        duration = time.time() - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration
        if txn_name not in self.latencies:
            self.latencies[txn_name] = []
        self.latencies[txn_name].append(duration)

        total_aborts = self.txn_aborts.get(txn_name, 0)
        self.txn_aborts[txn_name] = total_aborts
        total_retries = self.txn_retries.get(txn_name, 0)
        self.txn_retries[txn_name] = total_retries + retries
        if txn_name not in self.retries:
            self.retries[txn_name] = []
        self.retries[txn_name].append(retries)

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
            orig_aborts = self.txn_aborts.get(txn_name, 0)

            self.txn_counters[txn_name] = orig_cnt + r.txn_counters[txn_name]
            self.txn_mins[txn_name] = orig_min if orig_min < r.txn_mins[txn_name] else r.txn_mins[txn_name]
            self.txn_maxs[txn_name] = orig_max if orig_max > r.txn_maxs[txn_name] else r.txn_maxs[txn_name]
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            self.txn_retries[txn_name] = orig_retries + r.txn_retries[txn_name]
            self.txn_aborts[txn_name] = orig_aborts + r.txn_aborts[txn_name]
            # logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
            if txn_name not in self.latencies:
                self.latencies[txn_name] = []
            self.latencies[txn_name].extend(r.latencies[txn_name])
            if txn_name not in self.retries:
                self.retries[txn_name] = []
            self.retries[txn_name].extend(r.retries[txn_name])

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

        col_width = 16
        num_columns = 13
        total_width = (col_width*num_columns)-8
        f = "\n  " + (("%-" + str(col_width) + "s")*num_columns)
        line = "-"*total_width

        ret = u"\n" + "="*total_width + "\n"
        if load_time:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)

        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Complete", u"Time (µs)", u"Rate/thr", u"Retries", u"minLatMs", u"p50", u"p75", u"p90", u"p95", u"p99", u"maxLatMs", u"Aborts")

        total_time = 0
        total_cnt = 0
        total_retries = 0
        total_aborts = 0
        for txn in self.txn_counters:
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            total_time += txn_time
            total_cnt += txn_cnt

        for txn in sorted(self.txn_counters):
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            min_latency = u"%5.2f" % (1000 * self.txn_mins[txn])
            max_latency = u"%6.2f" % (1000 * self.txn_maxs[txn])
            samples = len(self.latencies[txn])
            lat = sorted(self.latencies[txn])
            ip50 = u"%6.2f" % (1000* lat[int(samples/2)])
            ip75 = u"%6.2f" % (1000*lat[int(samples/100.0*75)])
            ip90 = u"%6.2f" % (1000*lat[int(samples/100.0*90)])
            ip95 = u"%6.2f" % (1000*lat[int(samples/100.0*95)])
            ip99 = u"%6.2f" % (1000*lat[int(samples/100.0*99)])
            txn_aborts = self.txn_aborts[txn]
            total_aborts += txn_aborts
            txn_retries = self.txn_retries[txn]
            total_retries += txn_retries
            rate = u"%.02f txn/s" % ((txn_cnt / txn_time))
            #perc_cnt = u"%5.02f" % ((100.0*txn_cnt / total_cnt))
            #perc_time = u"%5.02f" % ((100.0*txn_time / total_time))
            just_retries = [x for x in self.retries[txn] if x>0]
            ret += f % (txn, str(txn_cnt), u"%9.3f" % (txn_time), rate, 
                        str(len(just_retries))+","+str(sum(just_retries))+","+str(100.00*txn_retries/txn_cnt)[:5]+"%",
                        min_latency, ip50, ip75, ip90, ip95, ip99, max_latency, txn_aborts)

        if 'NEW_ORDER' not in self.txn_counters:
            self.txn_counters['NEW_ORDER'] = 0
        ret += "\n" + line # ("-"*total_width)
        total_rate = "%.02f txn/s" % ((total_cnt / total_time))
        lat = sorted(self.latencies['NEW_ORDER'])
        samples = len(lat)
        ret += f % ("TOTAL", str(total_cnt), u"%12.3f" % total_time, total_rate, "", "", "", "", "", "", "", "", "")
        if driver != None:
            # print(driver)
            ret += "\n%s TpmC for %s %s thr %s txn %d WH: %d %d total %d durSec, batch %s %d retries %s%% %s fnM %s p50 %s p75 %s p90 %s p95 %s p99 %s max %s WC %s causal %s 10in1 %s retry %s %d %d" % (
                time.strftime("%Y-%m-%d %H:%M:%S"),
                ("normal", "denorm")[driver.denormalize],
                threads,
                ("with", "w/o ")[driver.no_transactions],
                warehouses,
                round(self.txn_counters['NEW_ORDER']*60/duration), self.txn_counters['NEW_ORDER'], duration,
                ("off", "on")[driver.batch_writes], total_retries, str(100.0*total_retries/txn_cnt)[:5],
                ("w/o ", "with")[driver.find_and_modify],
                driver.read_preference,
                u"%6.2f" % (1000* lat[int(samples/2)]), u"%6.2f" % (1000*lat[int(samples/100.0*75)]), 
                u"%6.2f" % (1000*lat[int(samples/100.0*90)]), u"%6.2f" % (1000*lat[int(samples/100.0*95)]),
                u"%6.2f" % (1000*lat[int(samples/100.0*99)]),
                u"%6.2f" % (1000.0*lat[-1]),
                str(driver.write_concern), ('false', 'true')[driver.causal_consistency],
                ('false', 'true')[driver.all_in_one_txn], ('false', 'true')[driver.retry_writes],txn_cnt,total_aborts)
        return ret.encode('ascii', "ignore")
## CLASS
