#!/usr/bin/python3.7
# -*- coding:utf-8 -*-
import cx_Oracle
import requests
from urllib.parse import urlencode

#한글 지원 방법
import os
os.putenv('NLS_LANG', '.UTF8')

#ORGA LIVE
# connection = cx_Oracle.connect('orgaeshop','orga80641','ORGA_LIVE')

#ORGA DEV
# connection = cx_Oracle.connect('orgamig','orgamig','ORGA_DEV')

#PULSHOP LIVE
# connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_LIVE')

#PULSHOP DEV
# connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_DEV')

cursor = connection.cursor()

def run_proc():
    cursor.execute("""
	UPDATE NPS_STOCK_LOC_CJ SET LOT4='20210506' WHERE SKU = '1900013' AND YMD = '20210505'
    """
    )
    # print(cursor.fetchone()[0])

if __name__ == "__main__":
    run_proc()
    connection.commit()
    cursor.close()
    connection.close()
