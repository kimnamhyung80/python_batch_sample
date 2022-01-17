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
# connection = cx_Oracle.connect('orgamig','orgamig','ORGA_DEV')
connection = cx_Oracle.connect('orgaeshop','orga80641','ORGA_LIVE')

cursor = connection.cursor()

def delete_null_data():
    cursor.execute("""
        -- DELETE MEM_STREET_ADDR_BOOK WHERE CONVERT_ADDR1 IS NULL
    """
    )

def totalCnt():
    cursor.execute("""
        SELECT COUNT(*) TOT_CNT
        FROM
	    MEM_ADDR_BOOK A
            LEFT OUTER JOIN MEM_STREET_ADDR_BOOK B ON (A.ADDR_BOOK_SEQ = B.ADDR_BOOK_SEQ)
        WHERE
            A.USE_YN = 'Y'
        AND REGEXP_LIKE(A.ADDR1, '(길|로) [0-9]')
        AND B.ADDR_BOOK_SEQ IS NULL
        AND A.ADDR1 IS NOT NULL
    """
    )
    return cursor.fetchone()[0]

def run_proc():
    cursor.execute("""
        SELECT A.ADDR_BOOK_SEQ, A.ADDR1
        FROM
	    MEM_ADDR_BOOK A
            LEFT OUTER JOIN MEM_STREET_ADDR_BOOK B ON (A.ADDR_BOOK_SEQ = B.ADDR_BOOK_SEQ)
        WHERE
            A.USE_YN = 'Y'
        AND REGEXP_LIKE(A.ADDR1, '(길|로) [0-9]')
        AND B.ADDR_BOOK_SEQ IS NULL
        AND A.ADDR1 IS NOT NULL
        AND ROWNUM <= 1000
    """
    )
    for ROWS in cursor.fetchall():
        # sample : http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=100&keyword=서울특별시 광진구 능동로31길 12-10&confmKey=devU01TX0FVVEgyMDIwMTAyMDExMDEyNzExMDMwNTY=&resultType=json
        try:
            URL = 'http://www.juso.go.kr/addrlink/addrLinkApi.do?currentPage=1&countPerPage=100&confmKey=' + apiKey + '&resultType=json'          
            req_addr = setReplace(ROWS[1])
            KEYWORD = { 'keyword' : req_addr }
            res = requests.get(URL + "&" + urlencode(KEYWORD))
            res_json = res.json()
            if int(res_json['results']['common']['totalCount']) == 0:
                insertBuilding(ROWS[0], ROWS[1], '', '', '')
            else:
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
                insertBuilding(ROWS[0], ROWS[1], res_addr1, bdMgtSn, zip_code)
        except TypeError as e:
            print("## Error : (" + str(ROWS[0]) + ") -> " + str(e))

def insertBuilding(addrBookSeq, req_addr1, res_addr1, buildingCode, zipCode):
    try:
        cursor.execute("""
            INSERT INTO MEM_STREET_ADDR_BOOK(ADDR_BOOK_SEQ, ADDR1, CONVERT_ADDR1, DELV_BUILDINGCODE, ZIPCODE)
            VALUES(:param1, :param2, :param3, :param4, :param5)
        """, param1=addrBookSeq, param2=req_addr1, param3=res_addr1, param4=buildingCode, param5=zipCode)
        connection.commit()
        print("-- " + str(addrBookSeq) + ", " + str(buildingCode))
    except TypeError as e:
        print("## Error : (" + str(addrBookSeq) + ") " + str(e))

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
