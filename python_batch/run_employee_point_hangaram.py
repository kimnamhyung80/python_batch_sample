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

#통합몰 DEV
# connection_mysql = pymysql.connect(user="dev_hangaram", passwd="vnfandnjs##77", host="db-665ft-kr.vpc-pub-cdb.ntruss.com", db="pulmuone_dev", charset="utf8")

#통합몰 LIVE
connection_mysql = pymysql.connect(user="prod_hangaram", passwd="vnfandnjs##77", host="db-6660f-kr.vpc-pub-cdb.ntruss.com", db="prod_pulmuone", charset="utf8")

cursor_pmo = connection.cursor()
cursor_hangaram = connection_mysql.cursor()

def run_proc():
  print("배치시작 ===============================================")
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "PMO 임직원 포인트 사용내역 조회!!")
  startDate = (datetime.now() + relativedelta(months=-1)).strftime("%Y-%m")
  # startDate = "2021-05"
  print("기준일자 : ", startDate)
  cursor_pmo.execute("""
    SELECT
    	EMPLOYEE_NUMBER EMPLOYEE_CD
    	, CHNN_NO
    	, TO_CHAR(USE_DT, 'YYYY-MM') USE_DT
    	, SUM(MEMBER_PRICE * GOODS_CNT) USE_PRICE
    FROM 
    	NPS_EXMEMBER_LIMIT_HIST
    WHERE 
    	USE_DT > TO_DATE(:dateValue||'-01', 'YYYY-MM-DD')
      -- AND CHNN_NO IN ('471', 'FD2', '533', 'LDS')
      -- AND CHNN_NO IN ('471', 'FD2', '533', 'LDS', 'CAF', 'babymeal', 'isslin', 'fulvita01', 'eatslim', '953')
      -- AND CHNN_NO = '234'
      -- AND CHNN_NO = 'YT00'
    GROUP BY EMPLOYEE_NUMBER, TO_CHAR(USE_DT, 'YYYY-MM'), CHNN_NO
  """, {"dateValue":startDate})
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "조회 완료!!")
  cursor_hangaram.execute("""
    DELETE FROM MG_EMPL_DISC_BATCH_TP_DATA WHERE USE_DT >= %s
  """, startDate)
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "MG_EMPL_DISC_BATCH_TP_DATA DELETE !!")
  count = 0
  for row in cursor_pmo.fetchall():
    cursor_hangaram.execute("""
      INSERT INTO MG_EMPL_DISC_BATCH_TP_DATA(EMPLOYEE_CD, CHNN_NO, USE_DT, USE_PRICE) VALUES(%s,%s,%s,%s)
    """, (row[0],row[1],row[2],row[3]))
    count = count + 1
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "MG_EMPL_DISC_BATCH_TP_DATA INSERT !! count : ", count)
  cursor_hangaram.execute("""
    DELETE FROM MG_EMPL_DISC WHERE USE_DT >= CONCAT(%s, '-01')
  """, startDate)
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "MG_EMPL_DISC DELETE !!")
  cursor_hangaram.execute("""
    INSERT INTO MG_EMPL_DISC(UR_ERP_EMPLOYEE_CD, PS_EMPL_DISC_GRP_ID, USE_DT, USE_PRICE)
    SELECT
      C.EMPLOYEE_CD
      , F.PS_EMPL_DISC_GRP_ID 
      , CONCAT(C.USE_DT, '-01')
      , SUM(C.USE_PRICE)
    FROM 
      MG_EMPL_DISC_BATCH_TP_DATA C
      INNER JOIN MG_EMPL_DISC_BATCH_TP_MAPPING D ON C.CHNN_NO = D.CHNN_NO_AS_IS
      INNER JOIN PS_EMPL_DISC_BRAND_GRP_BRAND E ON D.CHNN_NO_TO_BE = E.UR_BRAND_ID
      INNER JOIN UR_ERP_EMPLOYEE A ON C.EMPLOYEE_CD = A.UR_ERP_EMPLOYEE_CD
      INNER JOIN PS_EMPL_DISC_MASTER_LEGAL B ON A.ERP_REGAL_CD = B.ERP_REGAL_CD
      INNER JOIN PS_EMPL_DISC_GRP F ON B.PS_EMPL_DISC_MASTER_ID = F.PS_EMPL_DISC_MASTER_ID 
      INNER JOIN PS_EMPL_DISC_GRP_BRAND_GRP G ON E.PS_EMPL_DISC_BRAND_GRP_ID = G.PS_EMPL_DISC_BRAND_GRP_ID
    WHERE 
          F.PS_EMPL_DISC_GRP_ID = G.PS_EMPL_DISC_GRP_ID
      AND C.USE_DT >= %s
      AND D.REQ_TYPE = 'N'
    GROUP BY C.EMPLOYEE_CD, C.USE_DT, F.PS_EMPL_DISC_GRP_ID
  """, startDate)
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "MG_EMPL_DISC INSERT !!")
  print("Completed !!!")

if __name__ == "__main__":
    run_proc()
    connection_mysql.commit()
    cursor_pmo.close()
    cursor_hangaram.close()
    connection.close()
    connection_mysql.close()
