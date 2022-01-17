#!/usr/bin/python3.7
# -*- coding:utf-8 -*-
import cx_Oracle
import requests
import re
from urllib.parse import urlencode

#한글 지원 방법
import os
os.putenv('NLS_LANG', '.UTF8')

#API 연동키
#2021-07-29 일까지 사용가능
#신청페이지 : https://www.juso.go.kr/addrlink/devAddrLinkRequestWrite.do?returnFn=write&cntcMenu=URL
apiKey = 'devU01TX0FVVEgyMDIxMDQzMDEzMjAyNDExMTExMjg='
#연결에 필요한 기본 정보 (유저, 비밀번호, 데이터베이스 서버 주소)
connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_LIVE')
# connection = cx_Oracle.connect('NPMOSHOP','NPMOSHOP','PULSHOP_DEV')

cursor = connection.cursor()

def delete_null_data():
    cursor.execute("""
        -- DELETE NPS_DELV_STREET_ADDR_HIST WHERE CONVERT_DELV_STREET_ADDR1 IS NULL
    """
    )

def totalCnt():
    cursor.execute("""
        SELECT COUNT(*) TOT_CNT
        FROM
	    NPS_DELV_ADDR_HIST A
	    LEFT OUTER JOIN NPS_DELV_STREET_ADDR_HIST B ON (A.CUSTOMER_NUM = B.CUSTOMER_NUM AND A.PDAH_SEQ = B.PDAH_SEQ)
        WHERE
            A.ADDR_GUBUN_CD = '0001'
        AND REGEXP_LIKE(A.DELV_ADDR1, '(길|로) [0-9]')
        AND B.CUSTOMER_NUM IS NULL
        AND B.PDAH_SEQ IS NULL
    """
    )
    return cursor.fetchone()[0]

def run_proc():
    cursor.execute("""
        SELECT A.CUSTOMER_NUM, A.PDAH_SEQ, A.DELV_ADDR1 DELV_STREET_ADDR1
        FROM
	    NPS_DELV_ADDR_HIST A
	    LEFT OUTER JOIN NPS_DELV_STREET_ADDR_HIST B ON (A.CUSTOMER_NUM = B.CUSTOMER_NUM AND A.PDAH_SEQ = B.PDAH_SEQ)
        WHERE
            A.ADDR_GUBUN_CD = '0001'
        AND REGEXP_LIKE(A.DELV_ADDR1, '(길|로) [0-9]')
        AND B.CUSTOMER_NUM IS NULL
	-- AND A.CUSTOMER_NUM = '157766'
        AND B.PDAH_SEQ IS NULL
        AND ROWNUM <= 1000
    """
    )
    for ROWS in cursor.fetchall():
        # sample : http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=100&keyword=서울특별시 광진구 능동로31길 12-10&confmKey=devU01TX0FVVEgyMDIwMTAyMDExMDEyNzExMDMwNTY=&resultType=json
        try:
            URL = 'http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=100&confmKey=' + apiKey + '&resultType=json'
            req_addr = setReplace(ROWS[2])
            KEYWORD = { 'keyword' : req_addr }
            res = requests.get(URL + "&" + urlencode(KEYWORD))
            res_json = res.json()
            if int(res_json['results']['common']['totalCount']) == 0:
                insertBuilding(ROWS[0], ROWS[1], ROWS[2], '', '', '')
            else:
                addr1 = str(ROWS[2])
                addr1 = setReplace(addr1)
                bdMgtSn = ''
                res_addr1 = ''
                zip_code = ''
                for juso in res_json['results']['juso']:
                    if juso['buldSlno'] == '0':
                        if (juso['siNm'] + ' ' + juso['rn'] + ' ' + juso['buldMnnm']) == req_addr:
                            bdMgtSn = juso['bdMgtSn']
                            res_addr1 = juso['roadAddr']
                            zip_code = juso['zipNo']
                            break
                    else:
                        if (juso['siNm'] + ' ' + juso['rn'] + ' ' + juso['buldMnnm'] + '-' + juso['buldSlno']) == req_addr:
                            bdMgtSn = juso['bdMgtSn']
                            res_addr1 = juso['roadAddr']
                            zip_code = juso['zipNo']
                            break
                insertBuilding(ROWS[0], ROWS[1], ROWS[2], res_addr1, bdMgtSn, zip_code)
        except TypeError as e:
            print("## Error : (" + str(ROWS[0]) + ", " + str(ROWS[1]) + ") -> " + str(e))

def insertBuilding(customerNum, addrBookSeq, req_addr1, res_addr1, buildingCode, zipCode):
    try:
        cursor.execute("""
            INSERT INTO NPS_DELV_STREET_ADDR_HIST(CUSTOMER_NUM, PDAH_SEQ, DELV_STREET_ADDR1, CONVERT_DELV_STREET_ADDR1, DELV_BUILDINGCODE, ZIPCODE)
            VALUES(:param1, :param2, :param3, :param4, :param5, :param6)
        """, param1=customerNum, param2=addrBookSeq, param3=req_addr1, param4=res_addr1, param5=buildingCode, param6=zipCode)
        connection.commit()
        print("-- " + str(customerNum) + ", " + str(addrBookSeq) + ", " + str(buildingCode))
    except TypeError as e:
        print("## Error : (" + str(customerNum) + ", " + str(addrBookSeq) + ") " + str(e))

def setReplace(addr1):
    addr1 = addr1.replace(" -", "-").replace("- ", "-")
    regex = re.compile("([가-힣0-9]+[로|길] [\d-]+)")
    mc = regex.findall(addr1)
    if len(mc) > 0:
        addr1 = setSiNmReplace(addr1.split(" ")[0].strip()) + ' ' + mc[0]
    return addr1

def setSiNmReplace(addr1):
    addr1 = addr1.split(" ")[0].strip()
    if addr1 == '서울':
        addr1 = '서울특별시'
    elif addr1 == '부산' or addr1 == '대구' or addr1 == '인천' or addr1 == '광주' or addr1 == '대전' or addr1 == '울산':
        addr1 = addr1+'광역시'
    elif addr1 == '경기':
        addr1 = '경기도'
    elif addr1 == '강원':
        addr1 = '강원도'
    elif addr1 ==  '충북':
        addr1 = '충청북도'
    elif addr1 == '충남':
        addr1 = '충청남도'
    elif addr1 == '전북':
        addr1 = '전라북도'
    elif addr1 == '전남':
        addr1 = '전라남도'
    elif addr1 == '경북':
        addr1 = '경상북도'
    elif addr1 == '경남':
        addr1 = '경상남도'
    elif addr1 == '제주':
        addr1 = '제주도'
    return addr1

if __name__ == "__main__":
    # delete_null_data()
    totalCnt = totalCnt()
    print("###########################################")
    print("target total cnt : " + str(totalCnt))
    print("###########################################")
    totalPage = totalCnt // 1000 + 1
    for n in range(totalPage):
        run_proc()
        # break
    cursor.close()
    connection.close()
