# pytpcc/drivers/postgresqljsonbdriver.py

from __future__ import with_statement

import sys
import json
import traceback
from datetime import datetime
import psycopg2
import logging
from pprint import pformat
from time import sleep
from collections.abc import Sequence

import constants
from .abstractdriver import AbstractDriver

TXN_QUERIES = {
    "DELIVERY": {
        # "getNewOrder": "SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = %s AND NO_W_ID = %s AND NO_O_ID > -1 LIMIT 1", #
        "getNewOrder": "SELECT (data->>'NO_O_ID')::INTEGER FROM NEW_ORDER WHERE (data->>'NO_D_ID')::SMALLINT = %s AND (data->>'NO_W_ID')::SMALLINT = %s AND (data->>'NO_O_ID')::INTEGER > -1 LIMIT 1",
        # "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = %s AND NO_W_ID = %s AND NO_O_ID = %s", # d_id, w_id, no_o_id
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE (data->>'NO_D_ID')::SMALLINT = %s AND (data->>'NO_W_ID')::SMALLINT = %s AND (data->>'NO_O_ID')::INTEGER = %s",
        # "getCId": "SELECT O_C_ID FROM ORDERS WHERE O_ID = %s AND O_D_ID = %s AND O_W_ID = %s", # no_o_id, d_id, w_id
        "getCId": "SELECT (data->>'O_C_ID')::INTEGER FROM ORDERS WHERE (data->>'O_ID')::INTEGER = %s AND (data->>'O_D_ID')::SMALLINT = %s AND (data->>'O_W_ID')::SMALLINT = %s",
        # "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = %s WHERE O_ID = %s AND O_D_ID = %s AND O_W_ID = %s", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET data = jsonb_set(data, '{O_CARRIER_ID}', to_jsonb(%s::INTEGER)) WHERE (data->>'O_ID')::INTEGER = %s AND (data->>'O_D_ID')::SMALLINT = %s AND (data->>'O_W_ID')::SMALLINT = %s",
        # "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = %s WHERE OL_O_ID = %s AND OL_D_ID = %s AND OL_W_ID = %s", # o_entry_d, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET data = jsonb_set(data, '{OL_DELIVERY_D}', to_jsonb(%s::TIMESTAMP)) WHERE (data->>'OL_O_ID')::INTEGER = %s AND (data->>'OL_D_ID')::SMALLINT = %s AND (data->>'OL_W_ID')::SMALLINT = %s",
        # "sumOLAmount": "SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = %s AND OL_D_ID = %s AND OL_W_ID = %s", # no_o_id, d_id, w_id
        "sumOLAmount": "SELECT SUM((data->>'OL_AMOUNT')::FLOAT) FROM ORDER_LINE WHERE (data->>'OL_O_ID')::INTEGER = %s AND (data->>'OL_D_ID')::SMALLINT = %s AND (data->>'OL_W_ID')::SMALLINT = %s",
        # "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + %s WHERE C_ID = %s AND C_D_ID = %s AND C_W_ID = %s", # ol_total, c_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET data = jsonb_set(data, '{C_BALANCE}', to_jsonb(((data->>'C_BALANCE')::FLOAT + %s::FLOAT))) WHERE (data->>'C_ID')::INTEGER = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_W_ID')::SMALLINT = %s"
    },
    "NEW_ORDER": {
        # "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = %s", # w_id
        "getWarehouseTaxRate": "SELECT (data->>'W_TAX')::FLOAT FROM WAREHOUSE WHERE (data->>'W_ID')::SMALLINT = %s",
        # "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = %s AND D_W_ID = %s", # d_id, w_id
        "getDistrict": "SELECT (data->>'D_TAX')::FLOAT, (data->>'D_NEXT_O_ID')::INTEGER FROM DISTRICT WHERE (data->>'D_ID')::SMALLINT = %s AND (data->>'D_W_ID')::SMALLINT = %s",
        # "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = %s WHERE D_ID = %s AND D_W_ID = %s", # d_next_o_id, d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET data = jsonb_set(data, '{D_NEXT_O_ID}', to_jsonb(%s::INTEGER)) WHERE (data->>'D_ID')::SMALLINT = %s AND (data->>'D_W_ID')::SMALLINT = %s",
        # "getCustomer": "SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # w_id, d_id, c_id
        "getCustomer": "SELECT (data->>'C_DISCOUNT')::FLOAT, data->>'C_LAST', data->>'C_CREDIT' FROM CUSTOMER WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_ID')::INTEGER = %s",
        # "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::integer)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createOrder": "INSERT INTO ORDERS (data) VALUES (jsonb_build_object('O_ID', %s::INTEGER, 'O_D_ID', %s::SMALLINT, 'O_W_ID', %s::SMALLINT, 'O_C_ID', %s::INTEGER, 'O_ENTRY_D', %s::TIMESTAMP, 'O_CARRIER_ID', to_jsonb(%s), 'O_OL_CNT', %s::INTEGER, 'O_ALL_LOCAL', %s::INTEGER))",
        # "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (%s, %s, %s)", # o_id, d_id, w_id
        "createNewOrder": "INSERT INTO NEW_ORDER (data) VALUES (jsonb_build_object('NO_O_ID', %s::INTEGER, 'NO_D_ID', %s::SMALLINT, 'NO_W_ID', %s::SMALLINT))",
        # "getItemInfo": "SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = %s", # ol_i_id
        "getItemInfo": "SELECT (data->>'I_PRICE')::FLOAT, data->>'I_NAME', data->>'I_DATA' FROM ITEM WHERE (data->>'I_ID')::INTEGER = %s",
        # "getStockInfo": "SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_{:02d} FROM STOCK WHERE S_I_ID = %s AND S_W_ID = %s", # d_id, ol_i_id, ol_supply_w_id
        "getStockInfo": "SELECT (data->>'S_QUANTITY')::INTEGER, data->>'S_DATA', (data->>'S_YTD')::INTEGER, (data->>'S_ORDER_CNT')::INTEGER, (data->>'S_REMOTE_CNT')::INTEGER, data->>'S_DIST_{:02d}' FROM STOCK WHERE (data->>'S_I_ID')::INTEGER = %s AND (data->>'S_W_ID')::SMALLINT = %s",
        #"updateStock": "UPDATE STOCK SET S_QUANTITY = %s, S_YTD = %s, S_ORDER_CNT = %s, S_REMOTE_CNT = %s WHERE S_I_ID = %s AND S_W_ID = %s", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET data = jsonb_set(jsonb_set(jsonb_set(jsonb_set(data, '{S_QUANTITY}', to_jsonb(%s::INTEGER)), '{S_YTD}', to_jsonb(%s::INTEGER)), '{S_ORDER_CNT}', to_jsonb(%s::INTEGER)), '{S_REMOTE_CNT}', to_jsonb(%s::INTEGER)) WHERE (data->>'S_I_ID')::INTEGER = %s AND (data->>'S_W_ID')::SMALLINT = %s",
        # "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
        "createOrderLine": "INSERT INTO ORDER_LINE (data) VALUES (jsonb_build_object('OL_O_ID', %s::INTEGER, 'OL_D_ID', %s::SMALLINT, 'OL_W_ID', %s::SMALLINT, 'OL_NUMBER', %s::INTEGER, 'OL_I_ID', %s::INTEGER, 'OL_SUPPLY_W_ID', %s::SMALLINT, 'OL_DELIVERY_D', to_jsonb(%s), 'OL_QUANTITY', %s::INTEGER, 'OL_AMOUNT', %s::FLOAT, 'OL_DIST_INFO', %s::TEXT))"
    },
    
    "ORDER_STATUS": {
        # "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # w_id, d_id, c_id
        "getCustomerByCustomerId": "SELECT (data->>'C_ID')::INTEGER, data->>'C_FIRST', data->>'C_MIDDLE', data->>'C_LAST', (data->>'C_BALANCE')::FLOAT FROM CUSTOMER WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_ID')::INTEGER = %s",
        # "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST", # w_id, d_id, c_last
        "getCustomersByLastName": "SELECT (data->>'C_ID')::INTEGER, data->>'C_FIRST', data->>'C_MIDDLE', data->>'C_LAST', (data->>'C_BALANCE')::FLOAT FROM CUSTOMER WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND data->>'C_LAST' = %s ORDER BY data->>'C_FIRST'",
        # "getLastOrder": "SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = %s AND O_D_ID = %s AND O_C_ID = %s ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getLastOrder": "SELECT (data->>'O_ID')::INTEGER, (data->>'O_CARRIER_ID')::INTEGER, (data->>'O_ENTRY_D')::TIMESTAMP FROM ORDERS WHERE (data->>'O_W_ID')::SMALLINT = %s AND (data->>'O_D_ID')::SMALLINT = %s AND (data->>'O_C_ID')::INTEGER = %s ORDER BY (data->>'O_ID')::INTEGER DESC LIMIT 1",
        # "getOrderLines": "SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = %s AND OL_D_ID = %s AND OL_O_ID = %s", # w_id, d_id, o_id
        "getOrderLines": "SELECT (data->>'OL_SUPPLY_W_ID')::SMALLINT, (data->>'OL_I_ID')::INTEGER, (data->>'OL_QUANTITY')::INTEGER, (data->>'OL_AMOUNT')::FLOAT, (data->>'OL_DELIVERY_D')::TIMESTAMP FROM ORDER_LINE WHERE (data->>'OL_W_ID')::SMALLINT = %s AND (data->>'OL_D_ID')::SMALLINT = %s AND (data->>'OL_O_ID')::INTEGER = %s"
    },
    
    "PAYMENT": {
        # "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = %s", # w_id
        "getWarehouse": "SELECT data->>'W_NAME', data->>'W_STREET_1', data->>'W_STREET_2', data->>'W_CITY', data->>'W_STATE', data->>'W_ZIP' FROM WAREHOUSE WHERE (data->>'W_ID')::SMALLINT = %s",
        
        # "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + %s WHERE W_ID = %s", # h_amount, w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET data = jsonb_set(data, '{W_YTD}', to_jsonb(((data->>'W_YTD')::FLOAT + %s::FLOAT))) WHERE (data->>'W_ID')::SMALLINT = %s",
        
        # "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = %s AND D_ID = %s", # w_id, d_id
        "getDistrict": "SELECT data->>'D_NAME', data->>'D_STREET_1', data->>'D_STREET_2', data->>'D_CITY', data->>'D_STATE', data->>'D_ZIP' FROM DISTRICT WHERE (data->>'D_W_ID')::SMALLINT = %s AND (data->>'D_ID')::SMALLINT = %s",
        
        # "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + %s WHERE D_W_ID  = %s AND D_ID = %s", # h_amount, d_w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET data = jsonb_set(data, '{D_YTD}', to_jsonb(((data->>'D_YTD')::FLOAT + %s::FLOAT))) WHERE (data->>'D_W_ID')::SMALLINT = %s AND (data->>'D_ID')::SMALLINT = %s",
        
        # "getCustomerByCustomerId": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # w_id, d_id, c_id
        "getCustomerByCustomerId": "SELECT (data->>'C_ID')::INTEGER, data->>'C_FIRST', data->>'C_MIDDLE', data->>'C_LAST', data->>'C_STREET_1', data->>'C_STREET_2', data->>'C_CITY', data->>'C_STATE', data->>'C_ZIP', data->>'C_PHONE', (data->>'C_SINCE')::TIMESTAMP, data->>'C_CREDIT', (data->>'C_CREDIT_LIM')::FLOAT, (data->>'C_DISCOUNT')::FLOAT, (data->>'C_BALANCE')::FLOAT, (data->>'C_YTD_PAYMENT')::FLOAT, (data->>'C_PAYMENT_CNT')::INTEGER, data->>'C_DATA' FROM CUSTOMER WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_ID')::INTEGER = %s",
        
        # "getCustomersByLastName": "SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = %s AND C_D_ID = %s AND C_LAST = %s ORDER BY C_FIRST", # w_id, d_id, c_last
        "getCustomersByLastName": "SELECT (data->>'C_ID')::INTEGER, data->>'C_FIRST', data->>'C_MIDDLE', data->>'C_LAST', data->>'C_STREET_1', data->>'C_STREET_2', data->>'C_CITY', data->>'C_STATE', data->>'C_ZIP', data->>'C_PHONE', (data->>'C_SINCE')::TIMESTAMP, data->>'C_CREDIT', (data->>'C_CREDIT_LIM')::FLOAT, (data->>'C_DISCOUNT')::FLOAT, (data->>'C_BALANCE')::FLOAT, (data->>'C_YTD_PAYMENT')::FLOAT, (data->>'C_PAYMENT_CNT')::INTEGER, data->>'C_DATA' FROM CUSTOMER WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND data->>'C_LAST' = %s ORDER BY data->>'C_FIRST'",
        
        # "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = %s, C_YTD_PAYMENT = %s, C_PAYMENT_CNT = %s, C_DATA = %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateBCCustomer": "UPDATE CUSTOMER SET data = jsonb_set(jsonb_set(jsonb_set(jsonb_set(data, '{C_BALANCE}', to_jsonb(%s::FLOAT)), '{C_YTD_PAYMENT}', to_jsonb(%s::FLOAT)), '{C_PAYMENT_CNT}', to_jsonb(%s::INTEGER)), '{C_DATA}', to_jsonb(%s::TEXT)) WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_ID')::INTEGER = %s",
        
        # "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = %s, C_YTD_PAYMENT = %s, C_PAYMENT_CNT = %s WHERE C_W_ID = %s AND C_D_ID = %s AND C_ID = %s", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET data = jsonb_set(jsonb_set(jsonb_set(data, '{C_BALANCE}', to_jsonb(%s::FLOAT)), '{C_YTD_PAYMENT}', to_jsonb(%s::FLOAT)), '{C_PAYMENT_CNT}', to_jsonb(%s::INTEGER)) WHERE (data->>'C_W_ID')::SMALLINT = %s AND (data->>'C_D_ID')::SMALLINT = %s AND (data->>'C_ID')::INTEGER = %s",
        
        # "insertHistory": "INSERT INTO HISTORY VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        "insertHistory": "INSERT INTO HISTORY (data) VALUES (jsonb_build_object('H_C_ID', %s::INTEGER, 'H_C_D_ID', %s::SMALLINT, 'H_C_W_ID', %s::SMALLINT, 'H_D_ID', %s::SMALLINT, 'H_W_ID', %s::SMALLINT, 'H_DATE', %s::TIMESTAMP, 'H_AMOUNT', %s::FLOAT, 'H_DATA', %s::TEXT))"
    },
    
    "STOCK_LEVEL": {
        # "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = %s AND D_ID = %s", 
        "getOId": "SELECT (data->>'D_NEXT_O_ID')::INTEGER FROM DISTRICT WHERE (data->>'D_W_ID')::SMALLINT = %s AND (data->>'D_ID')::SMALLINT = %s",
        # "getStockCount": """
        #     SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
        #     WHERE OL_W_ID = %s
        #       AND OL_D_ID = %s
        #       AND OL_O_ID < %s
        #       AND OL_O_ID >= %s
        #       AND S_W_ID = %s
        #       AND S_I_ID = OL_I_ID
        #       AND S_QUANTITY < %s
        # """,
        "getStockCount": "SELECT COUNT(DISTINCT((ol.data->>'OL_I_ID')::INTEGER)) FROM ORDER_LINE ol, STOCK s WHERE (ol.data->>'OL_W_ID')::SMALLINT = %s AND (ol.data->>'OL_D_ID')::SMALLINT = %s AND (ol.data->>'OL_O_ID')::INTEGER < %s AND (ol.data->>'OL_O_ID')::INTEGER >= %s AND (s.data->>'S_W_ID')::SMALLINT = %s AND (s.data->>'S_I_ID')::INTEGER = (ol.data->>'OL_I_ID')::INTEGER AND (s.data->>'S_QUANTITY')::INTEGER < %s",
    },
}

TABLE_COLUMNS = {
    constants.TABLENAME_ITEM: [
        "I_ID", # INTEGER
        "I_IM_ID", # INTEGER
        "I_NAME", # VARCHAR
        "I_PRICE", # FLOAT
        "I_DATA", # VARCHAR
        "I_W_ID", # INTEGER
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

DENORMALIZED_TABLES = [
        constants.TABLENAME_ORDERS,
        constants.TABLENAME_ORDER_LINE
]

class PostgresqljsonbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("The name of the PostgreSQL database", "tpcc"),
        "host": ("The host address of the PostgreSQL server", "localhost"),
        "port": ("The port number of the PostgreSQL server", 5432),
        "user": ("The username to connect to the PostgreSQL database", "postgres"),
        "password": ("The password to connect to the PostgreSQL database", "")
        }

    def __init__(self, ddl):
        super(PostgresqljsonbDriver, self).__init__("postgresqljsonb", ddl)
        self.conn = None
        self.cursor = None
        self.denormalize = True
        self.w_orders = {}

    def makeDefaultConfig(self):
        return PostgresqljsonbDriver.DEFAULT_CONFIG

    def loadConfig(self, config):
        for key in PostgresqljsonbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.conn = psycopg2.connect(
            dbname=config["database"],
            user=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"]
        )

        self.cursor = self.conn.cursor()

        if config["reset"]:
            logging.info("Dropping all tables in database '%s'", config["database"])
            self.conn.autocommit = True  # Enable autocommit mode
            self.cursor.execute("""
                DO $$ DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                    END LOOP;
                END $$;
            """)
            self.conn.autocommit = False  # Disable autocommit mode

            logging.info("Restoring schema definitions in database '%s'", config["database"])
            with open(self.ddl, "r") as f:
                self.cursor.execute(f.read())
                self.conn.commit()

    def loadStart(self):
        """Disable constraints before data load
        and check default isolation level"""
        self.cursor.execute("SHOW TRANSACTION ISOLATION LEVEL")
        isolation_level = self.cursor.fetchone()[0]
        if isolation_level == "read committed":
            logging.warn("Read Committed isolation level could cause duplicate key errors, to avoid them use 'repeatable read'")

        self.cursor.execute("set session_replication_role to replica")

    def loadTuples(self, tableName, tuples):
        # if len(tuples) == 0:
        #     return
        # placeholders = ', '.join(['%s'] * len(tuples[0]))
        # sql = f"INSERT INTO {tableName} VALUES ({placeholders})"
        # self.cursor.executemany(sql, tuples)
        # logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        # return

        if not tuples:
            return
        logging.debug("Loading %d tuples for tableName %s", len(tuples), tableName)
        # print (tuples)
        # sys.exit()

        assert tableName in TABLE_COLUMNS, "Table %s not found in TABLE_COLUMNS" % tableName
        columns = TABLE_COLUMNS[tableName]
        num_columns = range(len(columns))

        tuple_dicts = []

        ## We want to combine all of a CUSTOMER's ORDERS, and ORDER_LINE records
        ## into a single document
        if self.denormalize and tableName in DENORMALIZED_TABLES:
            ## If this is the ORDERS table, then we'll just store the record locally for now
            if tableName == constants.TABLENAME_ORDERS:
                for t in tuples:
                    key = tuple(t[:1]+t[2:4]) # O_ID, O_C_ID, O_D_ID, O_W_ID
                    # self.w_orders[key] = dict(map(lambda i: (columns[i], t[i]), num_columns))
                    self.w_orders[key] = dict([(columns[i], t[i]) for i in num_columns])
                ## FOR
            ## IF

            ## If this is an ORDER_LINE record, then we need to stick it inside of the
            ## right ORDERS record
            elif tableName == constants.TABLENAME_ORDER_LINE:
                for t in tuples:
                    o_key = tuple(t[:3]) # O_ID, O_D_ID, O_W_ID
                    assert o_key in self.w_orders, "Order Key: %s\nAll Keys:\n%s" % (str(o_key), "\n".join(map(str, sorted(self.w_orders.keys()))))
                    o = self.w_orders[o_key]
                    if not tableName in o:
                        o[tableName] = []
                    o[tableName].append(dict([(columns[i], t[i]) for i in num_columns[4:]]))
                ## FOR

            ## Otherwise nothing
            else: assert False, "Only Orders and order lines are denormalized! Got %s." % tableName
        ## Otherwise just shove the tuples straight to the target collection
        else:
            if tableName == constants.TABLENAME_ITEM:
                tuples3 = []
                ww = [0]
                for t in tuples:
                    for w in ww:
                       t2 = list(t)
                       t2.append(w)
                       tuples3.append(t2)
                tuples = tuples3
            for t in tuples:
                tuple_dicts.append(dict([(columns[i], t[i]) for i in num_columns]))
            ## FOR
            # placeholders = ', '.join(['%s'] * len(tuples[0]))
            # 
            # is_sequence = isinstance(tuple_dicts, Sequence)
            # self.cursor.executemany(sql, tuple_dicts)
            sql = f"INSERT INTO {tableName} (data) VALUES (%s);"
            # data = [
            #     (json.dumps(d),)
            #     for d in tuple_dicts[:2]
            # ]

            #user default serializer method
            # def datetime_serializer(obj):
            #     if isinstance(obj, datetime):
            #         return obj.isoformat()
            #     raise TypeError("Type not serializable")
            class DateTimeEncoder(json.JSONEncoder):
                def default(self, obj):
                    if isinstance(obj, datetime):
                        return obj.isoformat()
                    # Call the default method for other object types
                    return super().default(obj)


            data = [(json.dumps(d, cls=DateTimeEncoder),) for d in tuple_dicts]
            self.cursor.executemany(sql, data)
            # self.conn.commit()
            # sys.exit(-1)
            # self.database[tableName].insert_many(tuple_dicts)
        ## IF

        return

    def loadFinish(self):
        self.cursor.execute("set session_replication_role to default;")
        logging.debug("Committing changes to database")
        self.conn.commit()
        logging.debug("Committing changes to database finished")

    def loadFinishDistrict(self, w_id, d_id):
        return
        if self.denormalize:
            logging.debug("Pushing %d denormalized ORDERS records for WAREHOUSE %d DISTRICT %d into MongoDB", len(self.w_orders), w_id, d_id)
            self.database[constants.TABLENAME_ORDERS].insert(self.w_orders.values())
            self.w_orders.clear()
        ## IF

    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        retries = 0
        q = TXN_QUERIES["DELIVERY"]
        
        w_id = params["w_id"]
        o_carrier_id = params["o_carrier_id"]
        ol_delivery_d = params["ol_delivery_d"]
        
        while True:
            try:
                result = [ ]
                for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                    self.cursor.execute(q["getNewOrder"], [d_id, w_id])
                    newOrder = self.cursor.fetchone()
                    if newOrder == None:
                        ## No orders for this district: skip it. Note: This must be reported if > 1%
                        continue
                    assert len(newOrder) > 0
                    no_o_id = newOrder[0]
                    
                    self.cursor.execute(q["getCId"], [no_o_id, d_id, w_id])
                    c_id = self.cursor.fetchone()[0]
                    
                    self.cursor.execute(q["sumOLAmount"], [no_o_id, d_id, w_id])
                    ol_total = self.cursor.fetchone()[0]

                    self.cursor.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
                    self.cursor.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
                    self.cursor.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])

                    # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                    # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                    # them out
                    # If there are no order lines, SUM returns null. There should always be order lines.
                    assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
                    assert ol_total > 0.0

                    self.cursor.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])

                    result.append((d_id, no_o_id))
                    ## FOR
                self.conn.commit()
                return (result,retries)
            except Exception as e:
                #print("An error occurred:")
                #traceback.print_exc()
                self.conn.rollback()  # Rollback the transaction on error
                retries += 1
                sleep(retries * .1)

    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        q = TXN_QUERIES["NEW_ORDER"]
        retries = 0
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        o_entry_d = params["o_entry_d"]
        i_ids = params["i_ids"]
        i_w_ids = params["i_w_ids"]
        i_qtys = params["i_qtys"]
            
        assert len(i_ids) > 0
        assert len(i_ids) == len(i_w_ids)
        assert len(i_ids) == len(i_qtys)

        all_local = True

        while True:
            try:
                items = [ ]
                for i in range(len(i_ids)):
                    ## Determine if this is an all local order or not
                    all_local = all_local and i_w_ids[i] == w_id
                    self.cursor.execute(q["getItemInfo"], [i_ids[i]])
                    items.append(self.cursor.fetchone())
                assert len(items) == len(i_ids)
                
                ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
                ## Note that this will happen with 1% of transactions on purpose.
                for item in items:
                    if item is None:
                        return (None, retries)
                ## FOR
                
                ## ----------------
                ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
                ## ----------------
                self.cursor.execute(q["getWarehouseTaxRate"], [w_id])
                w_tax = self.cursor.fetchone()[0]
                
                self.cursor.execute(q["getDistrict"], [d_id, w_id])
                district_info = self.cursor.fetchone()
                d_tax = district_info[0]
                d_next_o_id = district_info[1]
                
                self.cursor.execute(q["getCustomer"], [w_id, d_id, c_id])
                customer_info = self.cursor.fetchone()
                c_discount = customer_info[0]

                ## ----------------
                ## Insert Order Information
                ## ----------------
                ol_cnt = len(i_ids)
                o_carrier_id = constants.NULL_CARRIER_ID
                
                self.cursor.execute(q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
                self.cursor.execute(q["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
                self.cursor.execute(q["createNewOrder"], [d_next_o_id, d_id, w_id])

                ## ----------------
                ## Insert Order Item Information
                ## ----------------
                item_data = [ ]
                total = 0
                for i in range(len(i_ids)):
                    ol_number = i + 1
                    ol_supply_w_id = i_w_ids[i]
                    ol_i_id = i_ids[i]
                    ol_quantity = i_qtys[i]

                    itemInfo = items[i]
                    i_name = itemInfo[1]
                    i_data = itemInfo[2]
                    i_price = itemInfo[0]
                                
                    self.cursor.execute(q["getStockInfo"].format(d_id), [ol_i_id, ol_supply_w_id])
                    
                    stockInfo = self.cursor.fetchone()
                    if len(stockInfo) == 0:
                        logging.debug("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                        continue
                    s_quantity = stockInfo[0]
                    s_ytd = stockInfo[2]
                    s_order_cnt = stockInfo[3]
                    s_remote_cnt = stockInfo[4]
                    s_data = stockInfo[1]
                    s_dist_xx = stockInfo[5] # Fetches data from the s_dist_[d_id] column

                    ## Update stock
                    s_ytd += ol_quantity
                    if s_quantity >= ol_quantity + 10:
                        s_quantity = s_quantity - ol_quantity
                    else:
                        s_quantity = s_quantity + 91 - ol_quantity
                    s_order_cnt += 1
                    
                    if ol_supply_w_id != w_id: s_remote_cnt += 1

                    self.cursor.execute(q["updateStock"], [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id])

                    if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                        brand_generic = 'B'
                    else:
                        brand_generic = 'G'

                    ## Transaction profile states to use "ol_quantity * i_price"
                    ol_amount = ol_quantity * i_price
                    total += ol_amount

                    self.cursor.execute(q["createOrderLine"], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx])

                    ## Add the info to be returned
                    item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
                    ## FOR
                ## Commit!
                self.conn.commit()

                total *= (1 - c_discount) * (1 + w_tax + d_tax)

                ## Pack up values the client is missing (see TPC-C 2.4.3.5)
                misc = [ (w_tax, d_tax, d_next_o_id, total) ]
                return ([ customer_info, misc, item_data ], retries)
            except Exception as e:
                print("An error occurred:")
                traceback.print_exc()
                self.conn.rollback()  # Rollback the transaction on error
                retries += 1
                sleep(retries * .1)

    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES["ORDER_STATUS"]
        
        retries = 0
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        while True:
            try:
                if c_id != None:
                    self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
                    customer = self.cursor.fetchone()
                else:
                    # Get the midpoint customer's id
                    self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
                    all_customers = self.cursor.fetchall()
                    assert len(all_customers) > 0
                    namecnt = len(all_customers)
                    index = (namecnt-1)/2
                    customer = all_customers[int(index)]
                    c_id = customer[0]
                assert len(customer) > 0
                assert c_id != None

                self.cursor.execute(q["getLastOrder"], [w_id, d_id, c_id])
                order = self.cursor.fetchone()
                if order:
                    self.cursor.execute(q["getOrderLines"], [w_id, d_id, order[0]])
                    orderLines = self.cursor.fetchall()
                else:
                    orderLines = [ ]

                self.conn.commit()
                return ([ customer, order, orderLines ],retries)
            except Exception as e:
                print("An error occurred:")
                traceback.print_exc()
                self.conn.rollback()  # Rollback the transaction on error
                retries += 1
                sleep(retries * .1)

    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------    
    def doPayment(self, params):
        q = TXN_QUERIES["PAYMENT"]
        retries = 0
        w_id = params["w_id"]
        d_id = params["d_id"]
        h_amount = params["h_amount"]
        c_w_id = params["c_w_id"]
        c_d_id = params["c_d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        h_date = params["h_date"]

        while True:
            try:
                if c_id != None:
                    self.cursor.execute(q["getCustomerByCustomerId"], [w_id, d_id, c_id])
                    customer = self.cursor.fetchone()
                else:
                    # Get the midpoint customer's id
                    self.cursor.execute(q["getCustomersByLastName"], [w_id, d_id, c_last])
                    all_customers = self.cursor.fetchall()
                    assert len(all_customers) > 0
                    namecnt = len(all_customers)
                    index = (namecnt-1)/2
                    customer = all_customers[int(index)]
                    c_id = customer[0]
                assert len(customer) > 0
                c_balance = float(customer[14]) - h_amount #customer[14] is C_BALANCE which is float
                c_ytd_payment = (float(customer[15]) if customer[15] is not None else 0.0) + h_amount
                c_payment_cnt = (customer[16] if customer[16] is not None else 0) + 1
                c_data = customer[17]

                self.cursor.execute(q["getWarehouse"], [w_id])
                warehouse = self.cursor.fetchone()
                
                self.cursor.execute(q["getDistrict"], [w_id, d_id])
                district = self.cursor.fetchone()
                
                self.cursor.execute(q["updateWarehouseBalance"], [h_amount, w_id])
                self.cursor.execute(q["updateDistrictBalance"], [h_amount, w_id, d_id])

                # Customer Credit Information
                if customer[11] == constants.BAD_CREDIT:
                    newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                    c_data = (newData + "|" + c_data)
                    if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
                    self.cursor.execute(q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
                else:
                    c_data = ""
                    self.cursor.execute(q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])

                # Concatenate w_name, four spaces, d_name
                h_data = "%s    %s" % (warehouse[0], district[0])
                # Create the history record
                self.cursor.execute(q["insertHistory"], [c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data])

                self.conn.commit()

                # TPC-C 2.5.3.3: Must display the following fields:
                # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
                # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
                # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
                # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
                # H_AMOUNT, and H_DATE.

                # Hand back all the warehouse, district, and customer data
                return ([ warehouse, district, customer ],retries)
            except Exception as e:
                print("An error occurred:")
                traceback.print_exc()
                self.conn.rollback()  # Rollback the transaction on error
                retries += 1
                sleep(retries * .1)
        
    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES["STOCK_LEVEL"]
        retries = 0
        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        while True:
            try:
                self.cursor.execute(q["getOId"], [w_id, d_id])
                result = self.cursor.fetchone()
                assert result
                o_id = result[0]
                
                self.cursor.execute(q["getStockCount"], [w_id, d_id, o_id, (o_id - 20), w_id, threshold])
                result = self.cursor.fetchone()
                
                self.conn.commit()
                
                return (int(result[0]),retries)
            except Exception as e:
                print("An error occurred:")
                traceback.print_exc()
                self.conn.rollback()  # Rollback the transaction on error
                retries += 1
                sleep(retries * .1)

    ## ----------------------------------------------
    ## getNumberWH
    ## ----------------------------------------------    
    def getNumberWH(self):
        self.cursor.execute("SELECT MAX((data->'W_ID')::integer) FROM WAREHOUSE;")
        return self.cursor.fetchone()[0]
        
## CLASS