# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
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

from __future__ import with_statement

import os
import sys
import logging
import pymongo
import urllib
from pprint import pprint,pformat
from time import sleep

import constants
from abstractdriver import *

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
    ],
    constants.TABLENAME_WAREHOUSE: [
        "W_ID", # SMALLINT
        "W_NAME", # VARCHAR
        "W_STREET_1", # VARCHAR
        "W_STREET_2", # VARCHAR
        "W_CITY", # VARCHAR
        "W_STATE", # VARCHAR
        "W_ZIP", # VARCHAR
        "W_TAX", # FLOAT
        "W_YTD", # FLOAT
    ],
    constants.TABLENAME_DISTRICT: [
        "D_ID", # TINYINT
        "D_W_ID", # SMALLINT
        "D_NAME", # VARCHAR
        "D_STREET_1", # VARCHAR
        "D_STREET_2", # VARCHAR
        "D_CITY", # VARCHAR
        "D_STATE", # VARCHAR
        "D_ZIP", # VARCHAR
        "D_TAX", # FLOAT
        "D_YTD", # FLOAT
        "D_NEXT_O_ID", # INT
    ],
    constants.TABLENAME_CUSTOMER:   [
        "C_ID", # INTEGER
        "C_D_ID", # TINYINT
        "C_W_ID", # SMALLINT
        "C_FIRST", # VARCHAR
        "C_MIDDLE", # VARCHAR
        "C_LAST", # VARCHAR
        "C_STREET_1", # VARCHAR
        "C_STREET_2", # VARCHAR
        "C_CITY", # VARCHAR
        "C_STATE", # VARCHAR
        "C_ZIP", # VARCHAR
        "C_PHONE", # VARCHAR
        "C_SINCE", # TIMESTAMP
        "C_CREDIT", # VARCHAR
        "C_CREDIT_LIM", # FLOAT
        "C_DISCOUNT", # FLOAT
        "C_BALANCE", # FLOAT
        "C_YTD_PAYMENT", # FLOAT
        "C_PAYMENT_CNT", # INTEGER
        "C_DELIVERY_CNT", # INTEGER
        "C_DATA", # VARCHAR
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID", # INTEGER
        "S_W_ID", # SMALLINT
        "S_QUANTITY", # INTEGER
        "S_DIST_01", # VARCHAR
        "S_DIST_02", # VARCHAR
        "S_DIST_03", # VARCHAR
        "S_DIST_04", # VARCHAR
        "S_DIST_05", # VARCHAR
        "S_DIST_06", # VARCHAR
        "S_DIST_07", # VARCHAR
        "S_DIST_08", # VARCHAR
        "S_DIST_09", # VARCHAR
        "S_DIST_10", # VARCHAR
        "S_YTD", # INTEGER
        "S_ORDER_CNT", # INTEGER
        "S_REMOTE_CNT", # INTEGER
        "S_DATA", # VARCHAR
    ],
    constants.TABLENAME_ORDERS:     [
        "O_ID", # INTEGER
        "O_C_ID", # INTEGER
        "O_D_ID", # TINYINT
        "O_W_ID", # SMALLINT
        "O_ENTRY_D", # TIMESTAMP
        "O_CARRIER_ID", # INTEGER
        "O_OL_CNT", # INTEGER
        "O_ALL_LOCAL", # INTEGER
    ],
    constants.TABLENAME_NEW_ORDER:  [
        "NO_O_ID", # INTEGER
        "NO_D_ID", # TINYINT
        "NO_W_ID", # SMALLINT
    ],
    constants.TABLENAME_ORDER_LINE: [
        "OL_O_ID", # INTEGER
        "OL_D_ID", # TINYINT
        "OL_W_ID", # SMALLINT
        "OL_NUMBER", # INTEGER
        "OL_I_ID", # INTEGER
        "OL_SUPPLY_W_ID", # SMALLINT
        "OL_DELIVERY_D", # TIMESTAMP
        "OL_QUANTITY", # INTEGER
        "OL_AMOUNT", # FLOAT
        "OL_DIST_INFO", # VARCHAR
    ],
    constants.TABLENAME_HISTORY:    [
        "H_C_ID", # INTEGER
        "H_C_D_ID", # TINYINT
        "H_C_W_ID", # SMALLINT
        "H_D_ID", # TINYINT
        "H_W_ID", # SMALLINT
        "H_DATE", # TIMESTAMP
        "H_AMOUNT", # FLOAT
        "H_DATA", # VARCHAR
    ],
}

TABLE_INDEXES = {
    constants.TABLENAME_ITEM:       [
        "I_ID",
    ],
    constants.TABLENAME_WAREHOUSE:  [
        [("W_ID", pymongo.ASCENDING), ("W_TAX", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_DISTRICT:   [
        [("D_W_ID", pymongo.ASCENDING), ("D_ID", pymongo.ASCENDING), ("D_NEXT_O_ID", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_CUSTOMER:   [
        [("C_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("C_D_ID", pymongo.ASCENDING)],
        [("C_D_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("C_LAST", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_STOCK:      [
        "S_I_ID",
        [("S_W_ID", pymongo.ASCENDING), ("S_I_ID", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_ORDERS:     [
        [("O_ID", pymongo.ASCENDING), ("O_D_ID",pymongo.ASCENDING), ("O_W_ID",pymongo.ASCENDING), ("O_C_ID", pymongo.ASCENDING)],
        [("O_C_ID", pymongo.ASCENDING), ("O_D_ID",pymongo.ASCENDING), ("O_W_ID",pymongo.ASCENDING), ("O_ID",pymongo.DESCENDING), ("O_CARRIER_ID",pymongo.ASCENDING),("O_ENTRY_ID",pymongo.ASCENDING)]
    ],
    constants.TABLENAME_NEW_ORDER:  [
        [("NO_D_ID",pymongo.ASCENDING), ("NO_W_ID",pymongo.ASCENDING),  ("NO_O_ID", pymongo.ASCENDING), ("_id", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_ORDER_LINE: [
        [("OL_O_ID", pymongo.ASCENDING), ("OL_D_ID",pymongo.ASCENDING), ("OL_W_ID",pymongo.ASCENDING), ("OL_I_ID",pymongo.DESCENDING), ("OL_AMOUNT",pymongo.ASCENDING)]
    ],
}

DENORMALIZED_TABLE_INDEXES = {
    constants.TABLENAME_ITEM:       [    
        [("I_ID",pymongo.ASCENDING), ("STOCK.S_W_ID",pymongo.ASCENDING), ("STOCK.S_QUANTITY",pymongo.ASCENDING)]
    ],
    constants.TABLENAME_WAREHOUSE:  [
        [("W_ID",pymongo.ASCENDING), ("DISTRICT.D_ID",pymongo.ASCENDING)],
    ],
    constants.TABLENAME_CUSTOMER:   [
        [("C_D_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("C_LAST", pymongo.ASCENDING)],
        [("C_ID", pymongo.ASCENDING), ("C_D_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING)],
        [("C_D_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("ORDERS.O_ID", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_NEW_ORDER:  [
        [("NO_D_ID",pymongo.ASCENDING), ("NO_W_ID",pymongo.ASCENDING),  ("NO_O_ID", pymongo.ASCENDING), ("_id", pymongo.ASCENDING)]
    ],
    
}


## ==============================================
## MongodbDriver
## ==============================================
class MongodbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "uri":              ("The mongodb connection string or URI", "localhost:27017" ),
        "name":             ("Database name", "tpcc"),
        "denormalize":      ("If true, data will be denormalized using MongoDB schema design best practices", False),
        "notransactions":   ("If true, transactions will not be used (benchmarking only)", False),
        "findandmodify":    ("If true, new order will be fetched via findAndModify", False),
        "secondary_reads":  ("If true, we will perform causal reads against nearest if possible", False)
    }
    DENORMALIZED_TABLES = [
        constants.TABLENAME_CUSTOMER,
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE,
        constants.TABLENAME_HISTORY,
        constants.TABLENAME_ITEM,
        constants.TABLENAME_STOCK,
        constants.TABLENAME_WAREHOUSE,
        constants.TABLENAME_DISTRICT,
    ]


    def __init__(self, ddl):
        super(MongodbDriver, self).__init__("mongodb", ddl)
        self.noTransactions = False
        self.findAndModify = False
        self.database = None
        self.client = None
        self.executed=False
        self.session_opts = { }
        self.client_opts = { }
        self.w_customers = { }
        self.w_orders = { }
        self.w_warehouses = { }
        self.w_districts = { }
        self.w_stock = { }
        self.w_items = { }

        ## Create member mapping to collections
        for name in constants.ALL_TABLES:
            self.__dict__[name.lower()] = None
    ## DEF


    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return MongodbDriver.DEFAULT_CONFIG
    ## DEF


    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in MongodbDriver.DEFAULT_CONFIG.keys():
            # rather than forcing every value which has a default to be specified
            # we should pluck out the keys from default that are missing in config
            # and set them there to their default values
            if not key in config:
               logging.debug("'%s' not in %s conf, set to %s" % (key, self.name, str(MongodbDriver.DEFAULT_CONFIG[key][1])))
               config[key] = MongodbDriver.DEFAULT_CONFIG[key][1]

        self.secondary_reads = config['secondary_reads'] == 'True'
        self.session_opts["causal_consistency"] = False

        if self.secondary_reads:
            # The entire transaction--reads and writes--must execute against the primary
            # Print an error and exit if they want non-primary read preference
            print("Non-primary reads are not supported for this workload")
            sys.exit(1)
            # self.client_opts["read_preference"] = "nearest"
            # Let's explicitly enable causal if secondary reads are allowed
            # self.session_opts["causal_consistency"] = True
        else:
            self.client_opts["read_preference"] = "primary"
        ## IF

        temp_uri = config['uri']
        uri = "mongodb://" + urllib.quote_plus(config['user']) + ':' + urllib.quote_plus(config['passwd']) + '@' + temp_uri
        print("Going to connect to " + uri)

        self.client = pymongo.MongoClient(uri, readPreference=self.client_opts["read_preference"])

        self.database = self.client[str(config['name'])]
        self.denormalize = config['denormalize'] == 'True'
        self.noTransactions = config['notransactions'] == 'True'
        self.findAndModify = config['findandmodify'] == 'True'

        if self.denormalize: logging.debug("Using denormalized data model")

        if config["reset"]:
            logging.debug("Deleting database '%s'" % self.database.name)
            for name in constants.ALL_TABLES:
                    self.database[name].drop()
                    logging.debug("Dropped collection %s" % name)
            ## FOR
        ## IF

        ## Setup!
        load_indexes = ('execute' in config and not config['execute']) and \
                       ('load' in config and not config['load'])

        if not self.denormalize:
            for name in constants.ALL_TABLES:
                self.__dict__[name.lower()] = self.database[name]
                if load_indexes and name in TABLE_INDEXES:
                    for index in TABLE_INDEXES[name]:
                        self.database[name].create_index(index)
                ## IF
            ## FOR
        else:
            tables=[constants.TABLENAME_CUSTOMER, constants.TABLENAME_WAREHOUSE, constants.TABLENAME_ITEM,constants.TABLENAME_NEW_ORDER]
            for name in tables:
                self.__dict__[name.lower()] = self.database[name]
                if load_indexes and name in DENORMALIZED_TABLE_INDEXES:
                    for index in DENORMALIZED_TABLE_INDEXES[name]:
                        #print("CREATING INDEXES FOR", name, index)
                        self.database[name].create_index(index)
                    ## FOR
                ## IF
            ## FOR
        ## IF
    ## DEF


    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        logging.debug("Loading %d tuples for tableName %s" % (len(tuples), tableName))

        assert tableName in TABLE_COLUMNS, "Unexpected table %s" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))

        tuple_dicts = [ ]

        ## We want to combine all of a CUSTOMER's ORDERS, ORDER_LINE, and HISTORY records
        ## into a single document
        if self.denormalize and tableName in MongodbDriver.DENORMALIZED_TABLES:
            ## If this is the CUSTOMER table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_CUSTOMER:
                for t in tuples:
                    key = tuple(t[:3]) # C_ID, D_ID, W_ID
                    self.w_customers[key] = dict(map(lambda i: (columns[i], t[i]), num_columns))
                ## FOR
            ## IF

            ## If this is an ORDER_LINE record, then we need to stick it inside of the
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    (c_key, o_idx) = self.w_orders[o_key]
                    c = self.w_customers[c_key]
                    assert o_idx >= 0
                    assert o_idx < len(c[constants.TABLENAME_ORDERS])
                    o = c[constants.TABLENAME_ORDERS][o_idx]
                    if not tableName in o: o[tableName] = [ ]
                    o[tableName].append(dict(map(lambda i: (columns[i], t[i]), num_columns[4:])))
                ## FOR

            elif tableName == constants.TABLENAME_NEW_ORDER:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    (c_key, o_idx) = self.w_orders[o_key]
                    c = self.w_customers[c_key]
                    assert o_idx >= 0
                    assert o_idx < len(c[constants.TABLENAME_ORDERS])
                    o = c[constants.TABLENAME_ORDERS][o_idx]
                    if not tableName in o: o[tableName] = True
                ## FOR
                
            elif tableName == constants.TABLENAME_WAREHOUSE:
                for t in tuples:
                    k=t[0]
                    d=dict(map(lambda i: (columns[i], t[i]), num_columns))
                    if not k in self.w_warehouses: self.w_warehouses[k]=[]
                    self.w_warehouses[k].append(d)
                ## FOR

            elif tableName == constants.TABLENAME_DISTRICT:
                for t in tuples:
                    k=t[1]
                    if not k in self.w_districts: self.w_districts[k]=[]
                    d=dict(map(lambda i: (columns[i], t[i]), num_columns))
                    self.w_districts[k].append(d)
                ## FOR
                
            elif tableName == constants.TABLENAME_ITEM:
                for t in tuples:
                    k=t[0]
                    if not k in self.w_items: self.w_items[k] = []
                    self.w_items[k].append(dict(map(lambda i: (columns[i], t[i]), num_columns)))
                ## FOR
                
            elif tableName == constants.TABLENAME_STOCK:
                for t in tuples:
                    k=t[0]
                    if not k in self.w_stock: self.w_stock[k]=[]
                    self.w_stock[k].append(dict(map(lambda i: (columns[i], t[i]), num_columns)))
                ## FOR                
                               
            ## Otherwise we have to find the CUSTOMER record for the other tables
            ## and append ourselves to them
            else:
                if tableName == constants.TABLENAME_ORDERS:
                    key_start = 1
                    cols = num_columns[0:1] + num_columns[4:] # Removes O_C_ID, O_D_ID, O_W_ID
                else:
                    key_start = 0
                    cols = num_columns[3:] # Removes H_C_ID, H_C_D_ID, H_C_W_ID
                ## IF

                for t in tuples:
                    c_key = tuple(t[key_start:key_start+3]) # C_ID, D_ID, W_ID
                    assert c_key in self.w_customers, "Customer Key: %s\nAll Keys:\n%s" % (str(c_key), "\n".join(map(str, sorted(self.w_customers.keys()))))
                    c = self.w_customers[c_key]

                    if not tableName in c: c[tableName] = [ ]
                    c[tableName].append(dict(map(lambda i: (columns[i], t[i]), cols)))

                    ## Since ORDER_LINE doesn't have a C_ID, we have to store a reference to
                    ## this ORDERS record so that we can look it up later
                    if tableName == constants.TABLENAME_ORDERS:
                        o_key = (t[0], t[2], t[3]) # O_ID, O_D_ID, O_W_ID
                        self.w_orders[o_key] = (c_key, len(c[tableName])-1) # CUSTOMER, ORDER IDX
                    ## IF
                ## FOR
            ## IF

        ## Otherwise just shove the tuples straight to the target collection
        else:
            for t in tuples:
                tuple_dicts.append(dict(map(lambda i: (columns[i], t[i]), num_columns)))
            ## FOR

            self.database[tableName].insert(tuple_dicts)
        ## IF
        
        return
    ## DEF


    def get_count(self, collection, match={}, session=None):
        pipeline = [
            {
                "$match": match
            },
            {
                "$count": "count"
            }
        ]

        result = list(collection.aggregate(pipeline, session=session))
        if not result:
            return 0
        return result[0]['count']
    ## DEF


    def loadDataIntoDatabase(self):
        toDel=[]

        for w in self.w_warehouses:
            #print(self.w_warehouses[w])
            self.database[constants.TABLENAME_WAREHOUSE].insert(self.w_warehouses[w])
        ## FOR

        self.w_warehouses.clear()

        for w_id in self.w_districts:
            self.database[constants.TABLENAME_WAREHOUSE].update_one({"W_ID": w_id}, {"$push": {constants.TABLENAME_DISTRICT: {"$each": self.w_districts[w_id]}}})
            #print(w_id, self.w_districts[w_id])
            toDel.append(w_id)
        ## FOR

        for k in toDel:
            del self.w_districts[k]

        toDel=[] 

        for item in self.w_items:
            self.database[constants.TABLENAME_ITEM].insert(self.w_items[item])

        self.w_items.clear()

        for item_id in self.w_stock:
            self.database[constants.TABLENAME_ITEM].update_one({"I_ID": item_id}, {"$push": {constants.TABLENAME_STOCK: {"$each": self.w_stock[item_id]}}})
            toDel.append(item_id)
        ## FOR

        for k in toDel:
            del self.w_stock[k]

        #print("loadingData...")
    ## DEF


    def loadFinishItem(self):
        if self.denormalize:
            self.loadDataIntoDatabase()
    ## DEF


    def loadFinishWarehouse(self, w_id):
        if self.denormalize:
            self.loadDataIntoDatabase()
    ## DEF


    def loadFinishDistrict(self, w_id, d_id):
        if self.denormalize:
            logging.debug("Pushing %d denormalized CUSTOMER records for WAREHOUSE %d DISTRICT %d into MongoDB" % (len(self.w_customers), w_id, d_id))
            self.database[constants.TABLENAME_CUSTOMER].insert(self.w_customers.values())
            self.loadDataIntoDatabase()
            self.w_customers.clear()
            self.w_orders.clear()
        ## IF
    ## DEF


    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        return self.run_transaction_with_retries(self.client, self._doDeliveryTxn, "delivery", params)
    ## DEF


    def _doDeliveryTxn(self, s, params):
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]

        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
            ## getNewOrder
            if self.findAndModify:
                no = self.new_order.find_one_and_update({"NO_D_ID": d_id, "NO_W_ID": w_id}, {"$set":{"inProg":True}}, projection={"NO_O_ID": 1}, sort=[("NO_O_ID", 1)],session=s)
                if no == None:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
            else:
                no_cursor = self.new_order.find({"NO_D_ID": d_id, "NO_W_ID": w_id}, {"NO_O_ID": 1}, session=s).sort([("NO_O_ID", 1)]).limit(1)
                no_converted_cursor=list(no_cursor)
                if len(no_converted_cursor) == 0:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                ## IF
                no = no_converted_cursor[0]
           ## IF

            o_id = no["NO_O_ID"]
            assert o_id != None

            if self.denormalize:
                c=self.customer.find_one({"ORDERS.O_ID": o_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"C_ID": 1, "ORDERS.$": 1}, session=s)
                assert c!=None

                c_id = c["C_ID"]

                ## sumOLAmount + updateOrderLine
                ol_total = 0
                o=c["ORDERS"][0]
                o_id=o["O_ID"]
                orderLines = o["ORDER_LINE"]

                ol_total = sum([ol["OL_AMOUNT"] for ol in orderLines])

                if ol_total == 0:
                    pprint(params)
                    pprint(no)
                    pprint(c)
                    sys.exit(1)
                ## IF

                ## updateOrders + updateCustomer
                self.customer.update_one({"_id": c['_id'], "ORDERS.O_ID": o_id}, {"$set": {"ORDERS.$[o].O_CARRIER_ID": o_carrier_id, "ORDERS.$[o].ORDER_LINE.$[].OL_DELIVERY_D": ol_delivery_d}, "$inc": {"C_BALANCE": ol_total}}, array_filters=[{'o':{'O_ID':o_id}}],session=s)
            else:
                ## getCId
                o = self.orders.find_one({"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id}, {"O_C_ID": 1, "_id":0}, session=s)
                assert o != None
                c_id = o["O_C_ID"]

                ## sumOLAmount
                orderLines = self.order_line.find({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"_id":0, "OL_AMOUNT": 1}, session=s)
                assert orderLines != None
                ol_total = sum([ol["OL_AMOUNT"] for ol in orderLines])

                ## updateOrders
                self.orders.update_one(o, {"$set": {"O_CARRIER_ID": o_carrier_id}}, session=s)

                ## updateOrderLines
                self.order_line.update_many({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"$set": {"OL_DELIVERY_D": ol_delivery_d}}, session=s)

                ## updateCustomer
                self.customer.update_one({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"$inc": {"C_BALANCE": ol_total}}, session=s)

            ## IF

            ## deleteNewOrder
            self.new_order.delete_one({"_id": no['_id']}, session=s)

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0

            result.append((d_id, o_id))
        ## FOR

        return result
    ## DEF


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        return self.run_transaction_with_retries(self.client, self._doNewOrderTxn, "new order", params)
    ## DEF


    def _doNewOrderTxn(self, s, params):
        #print(self.w_stock, self.w_items, self.w_warehouses, self.w_districts, self.w_customers)

        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "S_DIST_%02d" % d_id

        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        ## http://stackoverflow.com/q/3844931/
        all_local = (not i_w_ids or [w_id] * len(i_w_ids) == i_w_ids)

        items = list(self.item.find({"I_ID": {"$in": i_ids}}, {"_id":0, "I_ID": 1, "I_PRICE": 1, "I_NAME": 1, "I_DATA": 1}, session=s))
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        if len(items) != len(i_ids):
            if s: s.abort_transaction()
            logging.debug("1% Abort transaction - expected")
            return
        ## IF

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------
        
        if self.denormalize:
            w = self.warehouse.find_one({"W_ID": w_id, "DISTRICT.D_ID": d_id}, {"_id":0, "W_TAX": 1, "DISTRICT.$": 1}, session=s)
            assert w
            w_tax=w["W_TAX"]
            d=w["DISTRICT"][0]
            d_tax = d["D_TAX"]
            d_next_o_id = d["D_NEXT_O_ID"]
            d["D_NEXT_O_ID"]=d_next_o_id+1
            self.warehouse.update_one({"W_ID": w_id, "DISTRICT.D_ID": d_id}, {"$set": {"DISTRICT.$": d}})
        else:
            # getWarehouseTaxRate
            w = self.warehouse.find_one({"W_ID": w_id}, {"_id":0, "W_TAX": 1}, session=s)
            assert w
            w_tax = w["W_TAX"]

            # getDistrict
            d = self.district.find_one({"D_ID": d_id, "D_W_ID": w_id}, {"_id":0, "D_TAX": 1, "D_NEXT_O_ID": 1}, session=s)
            assert d
            d_tax = d["D_TAX"]
            d_next_o_id = d["D_NEXT_O_ID"]

            # incrementNextOrderId
            self.district.update_one(d, {"$inc": {"D_NEXT_O_ID": 1}}, session=s)
        ## IF

        # getCustomer
        c = self.customer.find_one({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id}, {"C_DISCOUNT": 1, "C_LAST": 1, "C_CREDIT": 1}, session=s)
        assert c
        c_discount = c["C_DISCOUNT"]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID

        # createNewOrder
        
        self.new_order.insert_one({"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}, session=s)

        o = {"O_ID": d_next_o_id, "O_ENTRY_D": o_entry_d, "O_CARRIER_ID": o_carrier_id, "O_OL_CNT": ol_cnt, "O_ALL_LOCAL": all_local}

        if self.denormalize:
            o[constants.TABLENAME_ORDER_LINE] = [ ]
        else:
            o["O_D_ID"] = d_id
            o["O_W_ID"] = w_id
            o["O_C_ID"] = c_id

            # createOrder
            #print("Creating order:", o)
            self.orders.insert_one(o, session=s)
        ## IF

        ## ----------------
        ## OPTIMIZATION:
        ## If all of the items are at the same warehouse, then we'll issue a single
        ## request to get their information
        ## NOTE: NOT IMPLEMENTED
        ## ----------------
        stockInfos = None
        if all_local and False:
            # getStockInfo
            if not self.denormalize:
                allStocks = list(self.stock.find({"S_I_ID": {"$in": i_ids}, "S_W_ID": w_id}, {"_id":0, "S_I_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}, session=s))
                assert len(allStocks) == ol_cnt
            ## IF

            stockInfos = { }
        
            if self.denormalize:
                pass 
            else:
                for si in allStocks:
                    stockInfos["S_I_ID"] = si # HACK
            ## IF
        ## IF

        ## ----------------
        ## Insert Order Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo["I_NAME"]
            i_data = itemInfo["I_DATA"]
            i_price = itemInfo["I_PRICE"]
      
            if self.denormalize:
                allStock = self.item.find_one( {"I_ID": ol_i_id, "STOCK.S_W_ID": w_id}, {"_id":0, "STOCK.$": 1}, session=s)
                si = allStock["STOCK"][0]
            else:
                si = self.stock.find_one({"S_I_ID": ol_i_id, "S_W_ID": w_id}, {"_id":0, "S_I_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}, session=s)
            ## IF

            assert si, "Failed to find S_I_ID: %d\n%s" % (ol_i_id, pformat(itemInfo))

            s_quantity = si["S_QUANTITY"]
            s_ytd = si["S_YTD"]
            s_order_cnt = si["S_ORDER_CNT"]
            s_remote_cnt = si["S_REMOTE_CNT"]
            s_data = si["S_DATA"]
            s_dist_xx = si[s_dist_col] # Fetches data from the s_dist_[d_id] column

            ## Update stock
            s_ytd += ol_quantity
            if s_quantity >= ol_quantity + 10:
                s_quantity = s_quantity - ol_quantity
            else:
                s_quantity = s_quantity + 91 - ol_quantity
            ## IF

            s_order_cnt += 1

            if ol_supply_w_id != w_id: s_remote_cnt += 1

            # updateStock
            if self.denormalize:
                self.item.update_one({"I_ID": ol_i_id, "STOCK.S_W_ID": w_id}, {"$set": {"STOCK.$.S_QUANTITY": s_quantity, "STOCK.$.S_YTD": s_ytd, "STOCK.$.S_ORDER_CNT": s_order_cnt, "STOCK.$.S_REMOTE_CNT": s_remote_cnt}}, session=s)
            else:
                self.stock.update_one(si, {"$set": {"S_QUANTITY": s_quantity, "S_YTD": s_ytd, "S_ORDER_CNT": s_order_cnt, "S_REMOTE_CNT": s_remote_cnt}}, session=s)
            ## IF

            if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                brand_generic = 'B'
            else:
                brand_generic = 'G'
            ## IF

            ## Transaction profile states to use "ol_quantity * i_price"
            ol_amount = ol_quantity * i_price
            total += ol_amount

            ol = {"OL_O_ID": d_next_o_id, "OL_NUMBER": ol_number, "OL_I_ID": ol_i_id, "OL_SUPPLY_W_ID": ol_supply_w_id, "OL_DELIVERY_D": o_entry_d, "OL_QUANTITY": ol_quantity, "OL_AMOUNT": ol_amount, "OL_DIST_INFO": s_dist_xx}

            if self.denormalize:
                # createOrderLine
                o[constants.TABLENAME_ORDER_LINE].append(ol)
            else:
                ol["OL_D_ID"] = d_id
                ol["OL_W_ID"] = w_id

                # createOrderLine
                self.order_line.insert_one(ol, session=s)
            ## IF

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR

        ## Adjust the total for the discount
        #print "c_discount:", c_discount, type(c_discount)
        #print "w_tax:", w_tax, type(w_tax)
        #print "d_tax:", d_tax, type(d_tax)
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        if self.denormalize: #just added this, not sure what the functionality was before
            # createOrder
            #print(o)
            self.customer.update_one({"_id": c["_id"]}, {"$push": {"ORDERS": o}}, session=s)
        ## IF

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]

        return [ c, misc, item_data ]
    ## DEF


    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        return self.run_transaction_with_retries(self.client, self._doOrderStatusTxn, "order status", params)
    ## DEF


    def _doOrderStatusTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]

        assert w_id, pformat(params)
        assert d_id, pformat(params)

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id}
        return_fields = {"_id":0, "C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1}
        if self.denormalize:
            for f in ['O_ID', 'O_CARRIER_ID', 'O_ENTRY_D']:
                return_fields["%s.%s" % (constants.TABLENAME_ORDERS, f)] = 1
            for f in ['OL_SUPPLY_W_ID', 'OL_I_ID', 'OL_QUANTITY']:
                return_fields["%s.%s.%s" % (constants.TABLENAME_ORDERS, constants.TABLENAME_ORDER_LINE, f)] = 1
        ## IF

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields, session=s)
            assert c
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last

            all_customers = list(self.customer.find(search_fields, return_fields, session=s))
            namecnt = len(all_customers)
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        ## IF

        assert len(c) > 0
        assert c_id != None

        orderLines = [ ]
        order = None

        if self.denormalize:
            # getLastOrder
            if constants.TABLENAME_ORDERS in c:
                order = c[constants.TABLENAME_ORDERS][-1]
                # getOrderLines
                orderLines = order[constants.TABLENAME_ORDER_LINE]
            ## IF
        else:
            # getLastOrder
            order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id}, {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}, session=s).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
            o_id = order["O_ID"]

            if order:
                # getOrderLines
                orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id}, {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1}, session=s)
            ## IF
        ## IF

        return [ c, order, orderLines ]
    ## DEF


    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------
    def doPayment(self, params):
        return self.run_transaction_with_retries(self.client, self._doPaymentTxn, "payment", params)
    ## DEF


    def _doPaymentTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id}
        return_fields = {"C_BALANCE": 0, "C_YTD_PAYMENT": 0, "C_PAYMENT_CNT": 0}

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields, session=s)
            assert c
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last
            all_customers = list(self.customer.find(search_fields, return_fields, session=s))
            namecnt = len(all_customers) 
            assert namecnt > 0
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        ## IF

        assert len(c) > 0
        assert c_id != None

        c_data = c["C_DATA"]

        # getWarehouse
        w = self.warehouse.find_one({"W_ID": w_id}, {"W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1}, session=s)
        assert w

        # updateWarehouseBalance
        self.warehouse.update_one({"_id": w["_id"]}, {"$inc": {"H_AMOUNT": h_amount}}, session=s)

        if self.denormalize:
            # getDistrict
            d = self.warehouse.find_one( {"W_ID": w_id, "DISTRICT.D_ID": d_id}, {"_id":0, "DISTRICT.$": 1}, session=s)["DISTRICT"][0]
            assert d
        
            # updateDistrictBalance
            self.warehouse.update_one({"W_ID": w_id, "DISTRICT.D_ID": d_id},  {"$inc": {"DISTRICT.$.D_YTD": h_amount}}, session=s)
        else:
            # getDistrict
            d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1}, session=s)
            assert d

            # updateDistrictBalance
            self.district.update_one({"_id": d["_id"]},  {"$inc": {"D_YTD": h_amount}}, session=s)
        ## IF

        # Build CUSTOMER update command
        customer_update = {"$inc": {"C_BALANCE": h_amount*-1, "C_YTD_PAYMENT": h_amount, "C_PAYMENT_CNT": 1}}

        # Customer Credit Information
        if c["C_CREDIT"] == constants.BAD_CREDIT:
            newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
            c_data = (newData + "|" + c_data)
            if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
            customer_update["$set"] = {"C_DATA": c_data}
        ## IF

        # Concatenate w_name, four spaces, d_name
        h_data = "%s    %s" % (w["W_NAME"], d["D_NAME"])
            
        h = {"H_D_ID": d_id, "H_W_ID": w_id, "H_DATE": h_date, "H_AMOUNT": h_amount, "H_DATA": h_data}

        if self.denormalize:
            # insertHistory + updateCustomer
            customer_update["$push"] = {constants.TABLENAME_HISTORY: h}
            self.customer.update_one({"_id": c["_id"]}, customer_update, session=s)
        else:
            # updateCustomer
            self.customer.update_one({"_id": c["_id"]}, customer_update, session=s)

            # insertHistory
            self.history.insert_one(h, session=s)
        ## IF

        # TPC-C 2.5.3.3: Must display the following fields:
        # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
        # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
        # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
        # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
        # H_AMOUNT, and H_DATE.

        # Hand back all the warehouse, district, and customer data
        return [ w, d, c ]
    ## DEF


    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------
    def doStockLevel(self, params):
        return self.run_transaction_with_retries(self.client, self._doStockLevelTxn, "stock level", params)
    ## DEF


    def _doStockLevelTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]

        # getOId
        if self.denormalize:
            dis_name=constants.TABLENAME_DISTRICT
            d = self.warehouse.find_one({dis_name: {"$elemMatch": {"D_W_ID": w_id, "D_ID": d_id}}}, {"_id":0, "DISTRICT.$": 1}, session=s)[dis_name][0]

            if d==None:
                new_w=self.warehouse.find_one({"W_ID": w_id},session=s)

                for dis in new_w["DISTRICT"]:
                    try:
                        print("HAD ATTRIBUTES", dis["D_W_ID"],dis["D_ID"])
                    except:
                        print("DIDNT HAVE ATTRIBUTES",dis)
                ## FOR
            ## IF
        else:
            d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id}, {"_id":0, "D_NEXT_O_ID": 1}, session=s)
        ## IF

        assert d
        o_id = d["D_NEXT_O_ID"]

        # getStockCount
        # Outer Table: ORDER_LINE
        # Inner Table: STOCK
        if self.denormalize:
            c = self.customer.find({"C_W_ID": w_id, "C_D_ID": d_id, "ORDERS.O_ID": {"$lt": o_id, "$gte": o_id-20}}, {"ORDERS.ORDER_LINE.OL_I_ID": 1}, session=s)
            assert c

            orderLines = [ ]
            for ol in c:
                assert "ORDER_LINE" in ol["ORDERS"][0]
                orderLines.extend(ol["ORDERS"][0]["ORDER_LINE"])
            ## FOR
        else:
            orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"$lt": o_id, "$gte": o_id-20}}, {"OL_I_ID": 1}, session=s)
        ## IF

        assert orderLines
        ol_ids = set()
        for ol in orderLines:
            ol_ids.add(ol["OL_I_ID"])
        ## FOR

        if self.denormalize:
            result = self.get_count(self.item,{"I_ID": {"$in": list(ol_ids)}, "STOCK": {"$elemMatch": {"S_W_ID": w_id, "S_QUANTITY": {"$lt": threshold}}}}, s)
            logging.debug("Denormalized result of stock count is " + str(result))
        else:
            result = self.get_count(self.stock,
                                {"S_W_ID": w_id, "S_I_ID": {"$in": list(ol_ids)}, "S_QUANTITY": {"$lt": threshold}}, s)
            logging.debug("Normalized result of stock count is " + str(result))
        ## IF

        return int(result)
    ## DEF


    def run_transaction(self, client, txn_callback, session, name, params):
        if self.noTransactions: return (True, txn_callback(None, params))
        try:
            # this implicitly commits on success
            with session.start_transaction():
                return (True, txn_callback(session, params))
        except pymongo.errors.OperationFailure as exc:
            if exc.code in (24, 112, 244):  # LockTimeout, WriteConflict, TransactionAborted
                logging.debug("OperationFailure with error code: %d during operation: %s" % (exc.code, name))
                return (False, None)
            print "Failed with unknown OperationFailure: %d" % exc.code
            print(exc.details)
            raise
        except pymongo.errors.ConnectionFailure:
            print "ConnectionFailure during %s: " % name
            return (False, None)
        ## TRY
    ## DEF


    # Should we retry txns within the same session or start a new one?
    def run_transaction_with_retries(self, client, txn_callback, name, params):
        txn_counter = 0
        with client.start_session(causal_consistency=self.session_opts["causal_consistency"]) as s:
            while True:
                (ok, value) = self.run_transaction(client, txn_callback, s, name, params)
                if ok:
                    if txn_counter > 0:
                        logging.debug("Committed operation %s after %d retries" % (name, txn_counter))
                    return value
                ## IF

                # TODO: should we backoff a little bit before retry?
                txn_counter += 1
                sleep(txn_counter * .1)
                logging.debug("txn retry number for %s: %d" % (name, txn_counter))
            ## WHILE
    ## DEF

## CLASS
