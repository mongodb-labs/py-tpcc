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
        [("D_W_ID", pymongo.ASCENDING), ("D_ID", pymongo.ASCENDING), ("D_NEXT_O_ID", pymongo.ASCENDING), ("D_TAX", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_CUSTOMER:   [
        [("C_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("C_D_ID", pymongo.ASCENDING)],
        [("C_D_ID", pymongo.ASCENDING), ("C_W_ID", pymongo.ASCENDING), ("C_LAST", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_STOCK:      [
        [("S_W_ID", pymongo.ASCENDING), ("S_I_ID", pymongo.ASCENDING), ("S_QUANTITY", pymongo.ASCENDING)],
        "S_I_ID"
    ],
    constants.TABLENAME_ORDERS:     [
        [("O_W_ID", pymongo.ASCENDING), ("O_D_ID",pymongo.ASCENDING), ("O_ID",pymongo.ASCENDING), ("O_C_ID", pymongo.ASCENDING)],
        [("O_C_ID", pymongo.ASCENDING), ("O_D_ID",pymongo.ASCENDING), ("O_W_ID",pymongo.ASCENDING), ("O_ID",pymongo.DESCENDING), ("O_CARRIER_ID",pymongo.ASCENDING),("O_ENTRY_ID",pymongo.ASCENDING)]
    ],
    constants.TABLENAME_NEW_ORDER:  [
        [("NO_D_ID",pymongo.ASCENDING), ("NO_W_ID",pymongo.ASCENDING),  ("NO_O_ID", pymongo.ASCENDING)]
    ],
    constants.TABLENAME_ORDER_LINE: [
        [("OL_O_ID", pymongo.ASCENDING), ("OL_D_ID",pymongo.ASCENDING), ("OL_W_ID",pymongo.ASCENDING), ("OL_NUMBER",pymongo.ASCENDING)],
        [("OL_O_ID", pymongo.ASCENDING), ("OL_D_ID",pymongo.ASCENDING), ("OL_W_ID",pymongo.ASCENDING), ("OL_I_ID",pymongo.DESCENDING), ("OL_AMOUNT",pymongo.ASCENDING)]
    ],
}

## ==============================================
## MongodbDriver
## ==============================================
class MongodbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "uri":              ("The mongodb connection string or URI", "mongodb://localhost:27017" ),
        "name":             ("Database name", "tpcc"),
        "denormalize":      ("If true, data will be denormalized using MongoDB schema design best practices", True),
        "notransactions":   ("If true, transactions will not be used (benchmarking only)", False),
        "findandmodify":    ("If true, all things to update will be fetched via findAndModify", True),
        "secondary_reads":  ("If true, we will perform causal reads against nearest if possible", True)
    }
    DENORMALIZED_TABLES = [
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE
    ]


    def __init__(self, ddl):
        super(MongodbDriver, self).__init__("mongodb", ddl)
        self.noTransactions = False
        self.findAndModify = True
        self.database = None
        self.client = None
        self.executed=False
        self.session_opts = { }
        self.client_opts = { }
        self.w_orders = { }
        # things that are not better can't be set in config
        self.batchWrites = True
        self.agg = False
        self.allDeliveriesInOneTransaction = True

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

        self.session_opts["causal_consistency"] = True

        self.secondary_reads = config['secondary_reads'] == 'True'
        if self.secondary_reads:
            self.client_opts["read_preference"] = "secondaryPreferred"
        else:
            self.client_opts["read_preference"] = "primary"
        ## IF

        self.denormalize = config['denormalize'] == 'True'
        self.noTransactions = config['notransactions'] == 'True'
        self.findAndModify = config['findandmodify'] == 'True'
        self.writeConcern = pymongo.write_concern.WriteConcern(w=1)
        if 'write_concern' in config and config['write_concern'] and config['write_concern'] != '1':
             # only expecting string 'majority' as an alternative to w:1
             self.writeConcern = pymongo.write_concern.WriteConcern(w=str(config['write_concern']), wtimeout=30000)

        # handle building connection string
        # print config
        uri = None
        host = None
        user = None
        userpassword = ""
        if 'uri' in config:
            uri = config['uri']
        if 'host' in config:
            host = config['host']
        if 'user' in config:
            user = config['user']
            if not 'passwd' in config:
                logging.error("must specify password if user is specified")
                sys.exit(1)
            userpassword=urllib.quote_plus(user)+':'+urllib.quote_plus(config['passwd'])+"@"
        if uri and host:
            uri = None  # host overrides uri since that one has a default
        if uri:
            real_uri = uri
            if uri[0:14] == "mongodb+srv://":
                # print("got SRV")
                if userpassword:
                    real_uri = uri[0:14]+userpassword+uri[14:]
            if uri[0:10] == "mongodb://":
                # print("got regular URI")
                if userpassword:
                    real_uri = uri[0:10]+userpassword+uri[10:]
        elif not host:
            logging.error("must specify host if URI is not provided")
            sys.exit(1)
        else:
            # host provided
            if 'port' in config:
                host = host+':'+config['port']
            real_uri = "mongodb://" + userpassword + host

        try:
            if self.secondary_reads:
                 self.client = pymongo.MongoClient(real_uri, readPreference=self.client_opts["read_preference"], maxStalenessSeconds=90) 
            else:
                 self.client = pymongo.MongoClient(real_uri, readPreference=self.client_opts["read_preference"]) 
        except Exception, err:
            print "Was trying to connect to " + uri
            print "Got error " + str(err)
            return

        # set default writeConcern on the database
        self.database = self.client.get_database(name=str(config['name']), write_concern=self.writeConcern)
        if self.denormalize: logging.debug("Using denormalized data model")

        if config["reset"]:
            logging.info("Deleting database '%s'" % self.database.name)
            for name in constants.ALL_TABLES:
                    self.database[name].drop()
                    logging.debug("Dropped collection %s" % name)
            ## FOR
        ## IF

        ## whether should check for indexes
        load_indexes = ('execute' in config and not config['execute']) and \
                       ('load' in config and not config['load'])

        for name in constants.ALL_TABLES:
            if self.denormalize and name == "ORDER_LINE": continue
            self.__dict__[name.lower()] = self.database[name]
            if load_indexes and name in TABLE_INDEXES:
                uniq = True
                for index in TABLE_INDEXES[name]:
                    try:
                        self.database[name].create_index(index, unique=uniq)
                    except Exception, err:
                        print str(err)
                    uniq = False
            ## IF
        ## FOR
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
            ## If this is the ORDERS table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_ORDERS:
                for t in tuples:
                    key = tuple(t[:1]+t[2:4]) # O_ID, O_C_ID, O_D_ID, O_W_ID
                    self.w_orders[key] = dict(map(lambda i: (columns[i], t[i]), num_columns))
                ## FOR
            ## IF

            ## If this is an ORDER_LINE record, then we need to stick it inside of the
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    assert o_key in self.w_orders, "Order Key: %s\nAll Keys:\n%s" % (str(o_key), "\n".join(map(str, sorted(self.w_orders.keys()))))
                    o = self.w_orders[o_key]
                    if not tableName in o: o[tableName] = [ ]
                    o[tableName].append(dict(map(lambda i: (columns[i], t[i]), num_columns[4:])))
                ## FOR

            ## Otherwise nothing
            else: assert False, "Only Orders and order lines are denormalized! Got %s." % tableName
        ## Otherwise just shove the tuples straight to the target collection
        else:
            for t in tuples:
                tuple_dicts.append(dict(map(lambda i: (columns[i], t[i]), num_columns)))
            ## FOR

            self.database[tableName].insert(tuple_dicts)
        ## IF

        return
    ## DEF


    def loadFinishDistrict(self, w_id, d_id):
        if self.denormalize:
            logging.debug("Pushing %d denormalized ORDERS records for WAREHOUSE %d DISTRICT %d into MongoDB" % (len(self.w_orders), w_id, d_id))
            self.database[constants.TABLENAME_ORDERS].insert(self.w_orders.values())
            self.w_orders.clear()
        ## IF
    ## DEF


    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        # two options, option one (default) is to run a db transaction for each of 10 orders

        if self.allDeliveriesInOneTransaction:
            (value, retries) =  self.run_transaction_with_retries(self.client, self._doDelivery10Txn, "DELIVERY", params)
            #if retries > 0: print "DELIVERY had " + str(retries) + " retries"
            return (value, retries)
        result = [ ]
        retries = 0
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):    # there will be as many orders as districts per warehouse (10)
            params["d_id"]=d_id
            (r, rt) = self.run_transaction_with_retries(self.client, self._doDeliveryTxn, "DELIVERY", params)
            retries += rt
            result.append(r)
        #if retries > 0: print "10 DELIVERIES had " + str(retries) + " retries"
        return (result, retries)
    ## DEF

    def _doDelivery10Txn(self, s, params):
        result = [ ]
        for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):    # there will be as many orders as districts per warehouse (10)
            params["d_id"]=d_id
            r = self._doDeliveryTxn(s,params)
            if r:
                result.append(r)
        return result
    ## DEF

    def _doDeliveryTxn(self, s, params):
            w_id = params["w_id"]
            o_carrier_id = params["o_carrier_id"]
            ol_delivery_d = params["ol_delivery_d"]
            d_id = params["d_id"]
        # for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):    # there will be as many orders as districts per warehouse (10)
            comment = "DELIVERY " + str(d_id)
            ## getNewOrder
            if self.findAndModify:
                no = self.new_order.find_one_and_delete({"NO_D_ID": d_id, "NO_W_ID": w_id, "$comment": comment}, projection={"_id":0, "NO_D_ID":1, "NO_W_ID":1, "NO_O_ID": 1}, sort=[("NO_O_ID", 1)],session=s)
                if no == None:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    return None # continue
            else:
                no_cursor = self.new_order.find({"NO_D_ID": d_id, "NO_W_ID": w_id, "$comment": comment}, {"_id":0, "NO_D_ID":1, "NO_W_ID":1, "NO_O_ID": 1}, session=s).sort([("NO_O_ID", 1)]).limit(1)
                no_converted_cursor=list(no_cursor)
                if len(no_converted_cursor) == 0:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    return None  # continue
                ## IF
                no = no_converted_cursor[0]
           ## IF

            o_id = no["NO_O_ID"]
            assert o_id != None, "o_id cannot be missing for delivery"

            ## getCId
            if self.denormalize:
                if self.findAndModify:
                    o= self.orders.find_one_and_update({"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id, "$comment": comment}, {"$set": {"O_CARRIER_ID": o_carrier_id, "ORDER_LINE.$[].OL_DELIVERY_D": ol_delivery_d}}, session=s)
                else:
                    o = self.orders.find_one({"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id, "$comment": comment}, session=s)
            else:
                o = self.orders.find_one({"O_ID": o_id, "O_D_ID": d_id, "O_W_ID": w_id, "$comment": comment}, {"O_C_ID": 1, "O_ID": 1, "O_D_ID": 1, "O_W_ID": 1, "_id":0}, session=s)
            assert o != None, "o cannot be none, delivery"
            c_id = o["O_C_ID"]

            if self.denormalize:
                ## sumOLAmount + updateOrderLine
                ol_total = 0
                orderLines = o["ORDER_LINE"]

                ol_total = sum([ol["OL_AMOUNT"] for ol in orderLines])

                if ol_total == 0:
                    pprint(no)
                    pprint(c)
                    sys.exit(1)
                ## IF

                ## updateOrders 
                if not self.findAndModify: self.orders.update_one({"_id": o['_id'], "$comment": comment}, {"$set": {"O_CARRIER_ID": o_carrier_id, "ORDER_LINE.$[].OL_DELIVERY_D": ol_delivery_d}}, session=s)
            else:
                ## sumOLAmount
                orderLines = self.order_line.find({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id, "$comment": comment}, {"_id":0, "OL_AMOUNT": 1}, session=s)
                assert orderLines != None, "orderLines cannot be missing in delivery"
                ol_total = sum([ol["OL_AMOUNT"] for ol in orderLines])

                ## updateOrders
                o["$comment"] = comment
                self.orders.update_one(o, {"$set": {"O_CARRIER_ID": o_carrier_id}}, session=s)

                ## updateOrderLines
                self.order_line.update_many({"OL_O_ID": o_id, "OL_D_ID": d_id, "OL_W_ID": w_id}, {"$set": {"OL_DELIVERY_D": ol_delivery_d}}, session=s)

            ## IF

            ## updateCustomer
            self.customer.update_one({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id, "$comment": comment}, {"$inc": {"C_BALANCE": ol_total}}, session=s)

            ## deleteNewOrder
            # no["$comment"] = comment
            if not self.findAndModify: self.new_order.delete_one(no, session=s)

            # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
            # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
            # them out
            # If there are no order lines, SUM returns null. There should always be order lines.
            assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
            assert ol_total > 0.0, "ol_total is 0"

            return (d_id, o_id)   # result.append((d_id, o_id))
        ### FOR

        #return result
    ## DEF


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        (value, retries) = self.run_transaction_with_retries(self.client, self._doNewOrderTxn, "NEW_ORDER", params)
        return (value, retries)
    ## DEF


    def _doNewOrderTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
        s_dist_col = "S_DIST_%02d" % d_id
        comment = "NEW_ORDER"

        assert len(i_ids) > 0, "No matching i_ids found for new order"
        assert len(i_ids) == len(i_w_ids), "different number of i_ids and i_w_ids"
        assert len(i_ids) == len(i_qtys), "different number of i_ids and i_qtys"

        ## ----------------
        ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
        ## ----------------

        # getDistrict
        if self.findAndModify:
            d = self.district.find_one_and_update({"D_ID": d_id, "D_W_ID": w_id, "$comment": comment}, {"$inc":{"D_NEXT_O_ID":1}}, projection={"_id":0, "D_ID":1, "D_W_ID":1, "D_TAX": 1, "D_NEXT_O_ID": 1}, sort=[("NO_O_ID", 1)],session=s)
            assert d, "Couldn't find district in new order w_id %d d_id %d" % (w_id, d_id)
        else:
            d = self.district.find_one({"D_ID": d_id, "D_W_ID": w_id, "$comment": comment}, {"_id":0, "D_ID":1, "D_W_ID":1, "D_TAX": 1, "D_NEXT_O_ID": 1}, session=s)
            assert d, "Couldn't find district in new order w_id %d d_id %d" % (w_id, d_id)
            # incrementNextOrderId
            d["$comment"] = comment
            self.district.update_one(d, {"$inc": {"D_NEXT_O_ID": 1}}, session=s)
        ## IF
        d_tax = d["D_TAX"]
        d_next_o_id = d["D_NEXT_O_ID"]

        # fetch matching items and see if they are all valid
        items = list(self.item.find({"I_ID": {"$in": i_ids}, "$comment": comment}, {"_id":0, "I_ID": 1, "I_PRICE": 1, "I_NAME": 1, "I_DATA": 1}, session=s))
        ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
        ## Note that this will happen with 1% of transactions on purpose.
        if len(items) != len(i_ids):
            if not self.noTransactions: s.abort_transaction()
            logging.debug("1% Abort transaction (not all passed I_IDs are in ITEMS) - expected")
            return
        ## IF
        items=sorted(items, key=lambda x: i_ids.index(x['I_ID']))

        # getWarehouseTaxRate
        w = self.warehouse.find_one({"W_ID": w_id, "$comment": comment}, {"_id":0, "W_TAX": 1}, session=s)
        assert w, "Couldn't find warehouse in new order w_id %d" % (w_id)
        w_tax = w["W_TAX"]

        # getCustomer
        c = self.customer.find_one({"C_ID": c_id, "C_D_ID": d_id, "C_W_ID": w_id, "$comment": comment}, {"C_DISCOUNT": 1, "C_LAST": 1, "C_CREDIT": 1}, session=s)
        assert c, "Couldn't find customer in new order"
        c_discount = c["C_DISCOUNT"]

        ## ----------------
        ## Insert Order Information
        ## ----------------
        ol_cnt = len(i_ids)
        o_carrier_id = constants.NULL_CARRIER_ID

        # createNewOrder

        self.new_order.insert_one({"NO_O_ID": d_next_o_id, "NO_D_ID": d_id, "NO_W_ID": w_id}, session=s)

        all_local = (0, 1)[[w_id] * len(i_w_ids) == i_w_ids]
        o = {"O_ID": d_next_o_id, "O_ENTRY_D": o_entry_d, "O_CARRIER_ID": o_carrier_id, "O_OL_CNT": ol_cnt, "O_ALL_LOCAL": all_local}

        if self.denormalize:
            o[constants.TABLENAME_ORDER_LINE] = [ ]

        o["O_D_ID"] = d_id
        o["O_W_ID"] = w_id
        o["O_C_ID"] = c_id

        ## ----------------
        ## OPTIMIZATION:
        ## If all of the items are at the same warehouse, then we'll issue a single
        ## request to get their information, otherwise we'll still issue a single request
        ## ----------------
        item_w_list = zip(i_ids, i_w_ids)
        if all_local:
            allStocks = list(self.stock.find({"S_I_ID": {"$in": i_ids},"S_W_ID": w_id, "$comment": comment}, {"_id":0, "S_I_ID": 1, "S_W_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}, session=s))
        else:
            field_list = ["S_I_ID", "S_W_ID"]
            search_list = [dict(zip(field_list, ze)) for ze in item_w_list]
            allStocks = list(self.stock.find({"$or": search_list, "$comment": comment}, {"_id":0, "S_I_ID": 1, "S_W_ID": 1, "S_QUANTITY": 1, "S_DATA": 1, "S_YTD": 1, "S_ORDER_CNT": 1, "S_REMOTE_CNT": 1, s_dist_col: 1}, session=s))
        ## IF
        assert len(allStocks) == ol_cnt, "allStocks length != ol_cnt allStocks length %d and ol_cnt is %d" % (len(allStocks), ol_cnt)
        allStocks = sorted(allStocks, key=lambda x: item_w_list.index((x['S_I_ID'], x["S_W_ID"])))

        ## ----------------
        ## Insert Order Line, Stock Item Information
        ## ----------------
        item_data = [ ]
        total = 0
        # we already fetched all items so we should never need to go to self.item again
        # iterate over every line item
        # if self.batchWrites is set then write once per collection
        if self.batchWrites:
            stockWrites = []
            orderLineWrites = []
        ## IF
        for i in range(ol_cnt):
            ol_number = i + 1
            ol_supply_w_id = i_w_ids[i]
            ol_i_id = i_ids[i]
            ol_quantity = i_qtys[i]

            itemInfo = items[i]
            i_name = itemInfo["I_NAME"]
            i_data = itemInfo["I_DATA"]
            i_price = itemInfo["I_PRICE"]

            si = allStocks[i]

            assert si, "stock item not found"

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
            if self.batchWrites:
                si["$comment"] = comment
                stockWrites.append(pymongo.UpdateOne(si, {"$set": {"S_QUANTITY": s_quantity, "S_YTD": s_ytd, "S_ORDER_CNT": s_order_cnt, "S_REMOTE_CNT": s_remote_cnt}}))
            else:
                si["$comment"] = comment
                self.stock.update_one(si, {"$set": {"S_QUANTITY": s_quantity, "S_YTD": s_ytd, "S_ORDER_CNT": s_order_cnt, "S_REMOTE_CNT": s_remote_cnt}}, session=s)

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
                if self.batchWrites:
                    orderLineWrites.append(ol)
                else:
                    self.order_line.insert_one(ol, session=s)
                ## IF
            ## IF

            ## Add the info to be returned
            item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
        ## FOR

        ## Adjust the total for the discount
        total *= (1 - c_discount) * (1 + w_tax + d_tax)

        if self.batchWrites:
            if not self.denormalize: self.order_line.insert_many(orderLineWrites, session=s)
            self.stock.bulk_write(stockWrites, session=s)
        ## IF
        
        # createOrder
        self.orders.insert_one(o, session=s)

        ## Pack up values the client is missing (see TPC-C 2.4.3.5)
        misc = [ (w_tax, d_tax, d_next_o_id, total) ]

        return [ c, misc, item_data ]
    ## DEF


    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        #(value, retries) = self.run_transaction_with_retries(self.client, self._doOrderStatusTxn, "ORDER_STATUS", params)
        return (self._doOrderStatusTxn(None, params), 0)
    ## DEF


    def _doOrderStatusTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        comment = "ORDER_STATUS"

        assert w_id, pformat(params)
        assert d_id, pformat(params)

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id, "$comment": comment}
        return_fields = {"_id":0, "C_ID": 1, "C_FIRST": 1, "C_MIDDLE": 1, "C_LAST": 1, "C_BALANCE": 1}

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields, session=s)
            assert c, "Couldn't find customer in order status"
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last

            all_customers = list(self.customer.find(search_fields, return_fields, session=s))
            namecnt = len(all_customers)
            assert namecnt > 0, "No matching customer for last name %s!" % c_last
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        ## IF

        assert c_id != None, "Couldn't find c_id in order status"

        orderLines = [ ]
        order = None

        # getLastOrder
        if self.denormalize:
            order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id, "$comment": comment}, {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1, "ORDER_LINE":1}, session=s).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
        else:
            order = self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_C_ID": c_id, "$comment": comment}, {"O_ID": 1, "O_CARRIER_ID": 1, "O_ENTRY_D": 1}, session=s).sort("O_ID", direction=pymongo.DESCENDING).limit(1)[0]
        assert order, "No order found for customer!"
        o_id = order["O_ID"]

        # getOrderLines
        if self.denormalize:
            assert constants.TABLENAME_ORDER_LINE in order, "No ORDER_LINE in order %s" % repr(order)
            orderLines = order[constants.TABLENAME_ORDER_LINE]
        else:
            orderLines = self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": o_id, "$comment": comment}, {"OL_SUPPLY_W_ID": 1, "OL_I_ID": 1, "OL_QUANTITY": 1, "OL_AMOUNT": 1, "OL_DELIVERY_D": 1}, session=s)
        ## IF

        return [ c, order, orderLines ]
    ## DEF


    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------
    def doPayment(self, params):
        (value, retries) =  self.run_transaction_with_retries(self.client, self._doPaymentTxn, "PAYMENT", params)
        return (value, retries)
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
        comment = "PAYMENT"

        if self.findAndModify:
            w = self.warehouse.find_one_and_update({"W_ID": w_id, "$comment": comment}, {"$inc":{"W_YTD":h_amount}}, projection={"W_NAME":1,"W_STREET_1":1,"W_STREET_2":1,"W_CITY":1,"W_STATE":1,"W_ZIP":1}, session=s)
            assert w, "Couldn't find warehouse in payment w_id %d" % (w_id)
        else:
            # getWarehouse
            w = self.warehouse.find_one({"W_ID": w_id, "$comment": comment}, {"W_NAME": 1, "W_STREET_1": 1, "W_STREET_2": 1, "W_CITY": 1, "W_STATE": 1, "W_ZIP": 1}, session=s)
            assert w, "Couldn't find warehouse in payment w_id %d" % (w_id)
            # updateWarehouseBalance
            self.warehouse.update_one({"_id": w["_id"], "$comment": comment}, {"$inc": {"W_YTD": h_amount}}, session=s)
        ## IF

        # getDistrict
        if self.findAndModify:
            d = self.district.find_one_and_update({"D_ID": d_id, "D_W_ID": w_id, "$comment": comment}, {"$inc":{"D_YTD":h_amount}}, projection={"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1},session=s)
            assert d, "Couldn't find district in payment w_id %d d_id %d" % (w_id, d_id)
        else:
            d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id, "$comment": comment}, {"D_NAME": 1, "D_STREET_1": 1, "D_STREET_2": 1, "D_CITY": 1, "D_STATE": 1, "D_ZIP": 1}, session=s)
            assert d, "Couldn't find district in payment w_id %d d_id %d" % (w_id, d_id)
            # updateDistrictBalance
            self.district.update_one({"_id": d["_id"], "$comment": comment},  {"$inc": {"D_YTD": h_amount}}, session=s)
        ## IF

        search_fields = {"C_W_ID": w_id, "C_D_ID": d_id, "$comment": comment}
        return_fields = {"C_BALANCE": 0, "C_YTD_PAYMENT": 0, "C_PAYMENT_CNT": 0}

        if c_id != None:
            # getCustomerByCustomerId
            search_fields["C_ID"] = c_id
            c = self.customer.find_one(search_fields, return_fields, session=s)
            assert c, "Couldn't find customer in payment w_id %d d_id %d c_id %d" % (w_id, d_id, c_id)
        else:
            # getCustomersByLastName
            # Get the midpoint customer's id
            search_fields['C_LAST'] = c_last
            all_customers = list(self.customer.find(search_fields, return_fields, session=s)) # .sort([("NO_O_ID", 1)]) sort by C_FIRST is missing(!)
            namecnt = len(all_customers)
            assert namecnt > 0, "Didn't find any matching customers w_id %d d_id %d c_last %s" % (w_id, d_id, c_last)
            index = (namecnt-1)/2
            c = all_customers[index]
            c_id = c["C_ID"]
        ## IF

        assert c_id != None, "Didn't find any matching c_id"

        c_data = c["C_DATA"]

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

        # updateCustomer
        self.customer.update_one({"_id": c["_id"], "$comment": comment}, customer_update, session=s)

        # insertHistory
        self.history.insert_one(h, session=s)

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
    ## never requires transaction
    ## ----------------------------------------------
    def doStockLevel(self, params):
        return (self._doStockLevelTxn(None, params), 0)
    ## DEF


    def _doStockLevelTxn(self, s, params):
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        comment = "STOCK_LEVEL"

        if self.agg and self.denormalize:
            result = list(self.district.aggregate([
                   {"$match":{"D_W_ID":w_id, "D_ID":d_id}}, 
                   {"$limit":1},
                   {"$project":{"_id":0, "O_ID":{ "$range" : [ { "$subtract" : [ "$D_NEXT_O_ID", 20 ] }, "$D_NEXT_O_ID" ] }}},
                   {"$unwind" : "$O_ID"},
                   {"$lookup":{"from":"ORDERS", "as":"o", "let":{"oid":"$O_ID"}, "pipeline":[
                         {"$match":{"O_D_ID":d_id, "O_W_ID":w_id, "$expr":{"$eq":["$O_ID","$$oid"]}}},
                         {"$project":{"_id":0, "I_IDS":"$ORDER_LINE.OL_I_ID"}}
                   ]}}, 
                   {"$unwind" : "$o"},
                   {"$unwind" : "$o.I_IDS"},
                   {"$lookup":{"from":"STOCK", "as":"o", "let":{"ids":"$o.I_IDS"}, "pipeline":[
                         {"$match":{"S_W_ID":w_id, "S_QUANTITY": { "$lt": threshold }, "$expr":{"$eq":[ "$S_I_ID", "$$ids"]}}},
                         {"$project":{"S_W_ID":1}}
                   ]}},
                   {"$unwind":"$o"},
                   {"$count":"c"}
                 ]))
            if len(result) == 0: return 0
            return int(result[0]["c"])

        d = self.district.find_one({"D_W_ID": w_id, "D_ID": d_id, "$comment": comment}, {"_id":0, "D_NEXT_O_ID": 1}, session=s)

        assert d, "Didn't find matching district in stock level w_id %d d_id %d" % (w_id, d_id)
        o_id = d["D_NEXT_O_ID"]

        # getStockCount
        if self.denormalize:
            os = list(self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_ID": {"$lt": o_id, "$gte": o_id-20}, "$comment": comment}, {"ORDER_LINE.OL_I_ID": 1}, session=s))
            if len(os) == 0: 
                logging.warning("Didn't find matching orders in stock level w_id %d d_id %d o_id %d" % (w_id, d_id, o_id))
                # sleep one second and try again - TODO make it read from primary if it doesn't find anything here
                # if self read preference is secondary then try it from primary
                sleep(1)
                os = list(self.orders.find({"O_W_ID": w_id, "O_D_ID": d_id, "O_ID": {"$lt": o_id, "$gte": o_id-20}, "$comment": comment}, {"ORDER_LINE.OL_I_ID": 1}, session=s))
                logging.warning("still didn't find matching orders in stock level %d %d %d" % (w_id, d_id, o_id))
                assert os

            orderLines = [ ]
            for o in os:
                assert "ORDER_LINE" in o, "ORDER_LINE field not in order %d %d %d" % (w_id, d_id, o_id)
                orderLines.extend(o["ORDER_LINE"])
            ## FOR
        else:
            orderLines = list(self.order_line.find({"OL_W_ID": w_id, "OL_D_ID": d_id, "OL_O_ID": {"$lt": o_id, "$gte": o_id-20}, "$comment": comment}, {"_id":0, "OL_I_ID": 1},  batch_size=1000,session=s))
        ## IF

        assert orderLines, "orderLines should not be empty/null %d %d %d" % (w_id, d_id, o_id)
        ol_ids = set()
        for ol in orderLines:
            ol_ids.add(ol["OL_I_ID"])
        ## FOR

        result = self.stock.find({"S_W_ID": w_id, "S_I_ID": {"$in": list(ol_ids)}, "S_QUANTITY": {"$lt": threshold}, "$comment": comment}).count()

        return int(result)
    ## DEF


    def run_transaction(self, client, txn_callback, session, name, params):
        if self.noTransactions: return (True, txn_callback(session, params))
        try:
            # this implicitly commits on success
            with session.start_transaction():
                return (True, txn_callback(session, params))
        except pymongo.errors.OperationFailure as exc:
            if exc.has_error_label("TransientTransactionError"): # exc.code in (24, 112, 244):  # LockTimeout, WriteConflict, TransactionAborted
                logging.debug("OperationFailure with error code: %d (%s) during operation: %s" % (exc.code, exc.details, name))
                return (False, None)
            logging.error("Failed with unknown OperationFailure: %d" % exc.code)
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
        txn_retry_counter = 0
        to = pymongo.client_session.TransactionOptions(read_concern=None, write_concern=self.writeConcern, read_preference=pymongo.read_preferences.Primary())
        with client.start_session(default_transaction_options=to, causal_consistency=self.session_opts["causal_consistency"]) as s:
            while True:
                (ok, value) = self.run_transaction(client, txn_callback, s, name, params)
                if ok:
                    if txn_retry_counter > 0:
                        logging.debug("Committed operation %s after %d retries" % (name, txn_retry_counter))
                    if value is None: # account for the 1% aborted operations in the retries count
                        txn_retry_counter = txn_retry_counter+1
                    return (value, txn_retry_counter)
                ## IF

                # backoff a little bit before retry
                txn_retry_counter += 1
                sleep(txn_retry_counter * .1)
                logging.debug("txn retry number for %s: %d" % (name, txn_retry_counter))
            ## WHILE
    ## DEF

## CLASS
