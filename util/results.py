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

        self.txn_counters = { }
        self.txn_times = { }
        self.txn_retries = { }
        self.running = { }

    def startBenchmark(self):
        """Mark the benchmark as having been started"""
        assert self.start == None
        logging.debug("Starting benchmark statistics collection")
        self.start = time.time()
        return self.start

    def stopBenchmark(self):
        """Mark the benchmark as having been stopped"""
        assert self.start != None
        assert self.stop == None
        logging.debug("Stopping benchmark statistics collection")
        self.stop = time.time()

    def startTransaction(self, txn):
        self.txn_id += 1
        id = self.txn_id
        self.running[id] = (txn, time.time())
        return id

    def abortTransaction(self, id, retries=0):
        """Abort a transaction and discard its times"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]
        total_retries = self.txn_retries.get(txn_name, 0)
        self.txn_retries[txn_name] = total_retries + retries

    def stopTransaction(self, id, retries=0):
        """Record that the benchmark completed an invocation of the given transaction"""
        assert id in self.running
        txn_name, txn_start = self.running[id]
        del self.running[id]

        duration = time.time() - txn_start
        total_time = self.txn_times.get(txn_name, 0)
        self.txn_times[txn_name] = total_time + duration

        total_retries = self.txn_retries.get(txn_name, 0)
        self.txn_retries[txn_name] = total_retries + retries

        total_cnt = self.txn_counters.get(txn_name, 0)
        self.txn_counters[txn_name] = total_cnt + 1

    def append(self, r):
        for txn_name in r.txn_counters.keys():
            orig_cnt = self.txn_counters.get(txn_name, 0)
            orig_time = self.txn_times.get(txn_name, 0)
            orig_retries = self.txn_retries.get(txn_name, 0)

            self.txn_counters[txn_name] = orig_cnt + r.txn_counters[txn_name]
            self.txn_times[txn_name] = orig_time + r.txn_times[txn_name]
            self.txn_retries[txn_name] = orig_retries + r.txn_retries[txn_name]
            #logging.debug("%s [cnt=%d, time=%d]" % (txn_name, self.txn_counters[txn_name], self.txn_times[txn_name]))
        ## HACK
        self.start = r.start
        self.stop = r.stop

    def __str__(self):
        return self.show()

    def show(self, load_time = None, driver=None, threads=1):
        if self.start == None:
            return "Benchmark not started"
        if self.stop == None:
            duration = time.time() - self.start
        else:
            duration = self.stop - self.start

        col_width = 18
        num_columns = 8
        total_width = (col_width*num_columns)+2
        f = "\n  " + (("%-" + str(col_width) + "s")*num_columns)
        line = "-"*total_width

        ret = u"" + "="*total_width + "\n"
        if load_time != None:
            ret += "Data Loading Time: %d seconds\n\n" % (load_time)

        ret += "Execution Results after %d seconds\n%s" % (duration, line)
        ret += f % ("", "Executed", u"Time (µs)", u"Rate", u"Rate/Thread",u"% Count", u"% Time", u"# Retries")

        total_time = 0
        total_cnt = 0
        total_retries = 0
        for txn in self.txn_counters.keys():
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            total_time += txn_time
            total_cnt += txn_cnt

        for txn in sorted(self.txn_counters.keys()):
            txn_time = self.txn_times[txn]
            txn_cnt = self.txn_counters[txn]
            txn_retries = self.txn_retries[txn]
            total_retries += txn_retries
            rate = u"%.02f txn/s" % ((txn_cnt / txn_time))
            ratePerThread = u"%.02f txn/s" % ((txn_cnt / txn_time / threads))
            percCnt = u"%5.2f" % ( (100.0*txn_cnt / total_cnt) )
            percTime = u"%5.2f" % ( (100.0*txn_time / total_time) )
            ret += f % (txn, str(txn_cnt), str(txn_time), rate, ratePerThread, percCnt, percTime, str(txn_retries)+"/"+str(100.00*txn_retries/txn_cnt)[:5]+"%")

        ret += "\n" + ("-"*total_width)
        total_rate = "%.02f txn/s" % ((total_cnt / total_time))
        total_rate_per_thread = "%.02f txn/s" % ((total_cnt / total_time / threads))
        ret += f % ("TOTAL", str(total_cnt), str(total_time), total_rate, total_rate_per_thread, "", "", "")
        if driver != None:
            # print(driver)
            ret += "\n%s TpmC for %s, %s threads, %s txn %s findAndModify:  %d  (%d total orders %d sec duration, batch writes %s %d retries %s%%) " % (
                time.strftime("%Y-%m-%d %H:%M:%S"),
                ("normal", "denorm")[driver.denormalize],
                threads,
                ("with", "w/o ")[driver.noTransactions],
                ("w/o ", "with")[driver.findAndModify],
                round(self.txn_counters['NEW_ORDER']*60/duration), self.txn_counters['NEW_ORDER'], duration, 
                ("off", "on")[driver.batchWrites], total_retries, str(100.0*total_retries/total_cnt)[:5])

        return (ret.encode('ascii', "ignore"))
## CLASS
