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
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "#풀무원 임직원 포인트 사용내역 조회!!")
  startDate = (datetime.now() + relativedelta(months=-1)).strftime("%Y-%m")
  print("기준일자 : ", startDate)
  cursor_hangaram.execute("""
    SELECT 
    	A.ODID AS MST_ORD_NO
    	, A.UR_EMPLOYEE_CD AS EMPLOYEE_NUMBER
    	, C.IC_DT AS USE_DT
    	, E.CHNN_NO_AS_IS AS CHNN_NO
    	, B.ORDER_CNT - IFNULL(B.CANCEL_CNT, 0) AS GOODS_CNT
    	, B.IL_GOODS_ID AS GOODS_NO
    	, B.GOODS_NM AS GOODS_NM
    	, G.DISCOUNT_PRICE / B.ORDER_CNT AS MEMBER_PRICE
    	, I.DISCOUNT_RATIO AS MEMBER_RATE
    	, B.PAID_PRICE  / B.ORDER_CNT AS MEMBER_SELL_PRICE
    	, B.PAID_PRICE - IFNULL(K.CLAIM_DISCOUNT_PRICE, 0) AS TOTAL_PRICE
    	, B.OD_ORDER_DETL_SEQ AS POD_SEQ
    	, null AS SEGMENT1
    	, null AS SEGMENT2
    	, 'N' AS SETTLE_FG
    	, null AS SETTLE_DATE
    	, A.ODID AS ORD_NO
    FROM 
    	OD_ORDER A
    	INNER JOIN OD_ORDER_DETL B ON A.OD_ORDER_ID = B.OD_ORDER_ID
    	INNER JOIN OD_ORDER_DT C ON A.OD_ORDER_ID = C.OD_ORDER_ID
    	INNER JOIN OD_ORDER_DETL_DISCOUNT G ON B.OD_ORDER_ID = G.OD_ORDER_ID AND B.OD_ORDER_DETL_ID = G.OD_ORDER_DETL_ID
    	INNER JOIN MG_EMPL_DISC_BATCH_TP_MAPPING E ON G.UR_BRAND_ID = E.CHNN_NO_TO_BE
    	INNER JOIN PS_EMPL_DISC_GRP_BRAND_GRP H ON G.PS_EMPL_DISC_GRP_ID = H.PS_EMPL_DISC_GRP_ID 
    	INNER JOIN PS_EMPL_DISC_BRAND_GRP I ON H.PS_EMPL_DISC_BRAND_GRP_ID = I.PS_EMPL_DISC_BRAND_GRP_ID
     	INNER JOIN PS_EMPL_DISC_BRAND_GRP_BRAND J ON I.PS_EMPL_DISC_BRAND_GRP_ID = J.PS_EMPL_DISC_BRAND_GRP_ID
     	LEFT OUTER JOIN (
    	 		SELECT B.OD_ORDER_ID, C.OD_ORDER_DETL_ID, SUM(C.PAID_PRICE) CLAIM_DISCOUNT_PRICE
    			FROM 
    				OD_CLAIM B
    				INNER JOIN OD_CLAIM_DETL C ON C.OD_CLAIM_ID = B.OD_CLAIM_ID 
    			GROUP BY B.OD_ORDER_ID, C.OD_ORDER_DETL_ID
    		) K ON (B.OD_ORDER_ID = K.OD_ORDER_ID AND B.OD_ORDER_DETL_ID = K.OD_ORDER_DETL_ID)
    WHERE 
    	A.UR_EMPLOYEE_CD IS NOT NULL
    	AND A.UR_EMPLOYEE_CD <> ''
    	AND E.REQ_TYPE = 'R'
    	AND G.DISCOUNT_TP = 'GOODS_DISCOUNT_TP.EMPLOYEE'
    	AND J.UR_BRAND_ID = G.UR_BRAND_ID
        AND (B.ORDER_CNT - IFNULL(B.CANCEL_CNT, 0)) > 0
  """)
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "조회 완료!!")
  cursor_pmo.execute("DELETE FROM HGRM_EXMEMBER_LIMIT_HIST")
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "HGRM_EXMEMBER_LIMIT_HIST DELETE !!")
  count = 0
  for row in cursor_hangaram.fetchall():
    cursor_pmo.execute("""
      INSERT INTO HGRM_EXMEMBER_LIMIT_HIST(
        MST_ORD_NO
        , EMPLOYEE_NUMBER
        , USE_DT
        , CHNN_NO
        , GOODS_CNT
        , GOODS_NO
        , GOODS_NM
        , MEMBER_PRICE
        , MEMBER_RATE
        , MEMBER_SELL_PRICE
        , TOTAL_PRICE
        , POD_SEQ
        , SEGMENT1
        , SEGMENT2
        , SETTLE_FG
        , SETTLE_DATE
        , ORD_NO
      )
    VALUES(:param0,:param1,:param2,:param3,:param4,:param5,:param6,:param7,:param8,:param9,:param10,:param11,:param12,:param13,:param14,:param15,:param16)
    """, {"param0":row[0], "param1":row[1], "param2":row[2], "param3":row[3], "param4":row[4], "param5":row[5], "param6":row[6], "param7":row[7], "param8":row[8], "param9":row[9], "param10":row[10], "param11":row[11], "param12":row[12], "param13":row[13], "param14":row[14], "param15":row[15], "param16":row[16]})
    count = count + 1
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "HGRM_EXMEMBER_LIMIT_HIST INSERT !! count : ", count)
  cursor_pmo.execute("""
    UPDATE /*+ bypass_ujvc */
    (
    	SELECT A.SEGMENT1, A.SEGMENT2, B.SEGMENT1 AS ST1, B.SEGMENT2 AS ST2
    	FROM HGRM_EXMEMBER_LIMIT_HIST A, NPS_EXMEMBER B
    	WHERE A.EMPLOYEE_NUMBER = B.EMPLOYEE_NUMBER
    )
    SET SEGMENT1 = ST1, SEGMENT2 = ST2
  """)
  connection.commit()
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "] ", "HGRM_EXMEMBER_LIMIT_HIST SEGMENT1, SEGMENT2 UPDATE!!")
  print("[", datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), "]  Completed !!!")

if __name__ == "__main__":
    run_proc()
    connection_mysql.commit()
    cursor_pmo.close()
    cursor_hangaram.close()
    connection.close()
    connection_mysql.close()
