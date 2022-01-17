#!/usr/bin/python3.7
# -*- coding:utf-8 -*-
import cx_Oracle
import requests
import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from urllib.parse import urlencode

#한글 지원 방법
import os
os.putenv('NLS_LANG', '.UTF8')

#PULSHOP LIVE
connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_LIVE')

#PULSHOP DEV
# connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_DEV')

cursor_pmo = connection.cursor()

def run_proc():
  print("배치시작 ===============================================")
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "PMO 임직원 포인트 사용내역 조회!!")
  startDate = (datetime.now() + relativedelta(months=-2)).strftime("%Y%m")+"01"
  print(startDate)
  cursor_pmo.execute("""
    SELECT
    	COUNT(*) CNT
    FROM
    	NPS_EXMEMBER_LIMIT_HIST
    WHERE
    	EMPLOYEE_NUMBER = :dateValue
    GROUP BY EMPLOYEE_NUMBER, TO_CHAR(USE_DT, 'YYYY-MM'), CHNN_NO
  """, {"dateValue":"test"})
  row = cursor_pmo.fetchall()
  print(row[0])

if __name__ == "__main__":
    run_proc()
    cursor_pmo.close()
    connection.close()
