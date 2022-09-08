# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 13:03:01 2021

@author: user
"""

from urllib.request import urlopen 
from urllib.parse import urlencode, unquote, quote_plus 
import urllib 
import requests 
import json 
import pandas as pd
import os
import sys
from datetime import datetime, timedelta
import platform
import paramiko
import numpy as np
import csv
import openpyxl

from xlwt import Workbook
import io


# Init -----------------------------------------------------------------------------------------------------------------
# OS에 따라 path 변경
uploadIP = '172.90.xx.xxx'

if platform.system() == 'Windows':
    #filePath = "D:/dfibiz/trn/01.개발/05.GA/weather_asos_" + s_yyyymmdd + ".txt"
    filePath = "C:/Users/user/Desktop/파이썬_test/DATA/"
    dirPath = "D:/dfibiz/trn/01.개발/05.GA/"
elif platform.system() == 'Linux':
    #filePath = "/DATA/CRAWLING/weather_asos_" + s_yyyymmdd + ".txt"
    filePath = "/DATA/WEATHER/"
    dirPath = "/data/DATASTAGE/WEATHER/"

# GetWeek ------------------------------------------------------------------------------------------------------------
def GetWeek(today):
    try:
        firstDay = today.replace(day=1)
        if firstDay.weekday() == 6:
            origin = firstDay
        elif firstDay.weekday() < 3:
            origin = firstDay - timedelta(days=firstDay.weekday() + 1)
        else:
            origin = firstDay + timedelta(days=6 - firstDay.weekday())
        return int((today - origin).days / 7 + 1)
    except Exception as e:
        print("[GetWeek()] - ", e)

# GetDayWeek ------------------------------------------------------------------------------------------------------------
def GetDayWeek(today):
    try:
        if today.weekday() == 0:
            return '월요일'
        elif today.weekday() == 1:
            return '화요일'
        elif today.weekday() == 2:
            return '수요일'
        elif today.weekday() == 3:
            return '목요일'
        elif today.weekday() == 4:
            return '금요일'
        elif today.weekday() == 5:
            return '토요일'
        elif today.weekday() == 6:
            return '일요일'
        else:
            return ''
    except Exception as e:
        print("[GetDayWeek()] - ", e)

# 엑셀 좌표파일 읽기 -------------------------------------------------------------------------------------------------------------

KEYWORD = []

def fileOpen():
    try:
        #filenm = '기상청18_동네예보 조회서비스_오픈API활용가이드_격자_위경도(20210106).xlsx'
        
        #file = open(filePath+filenm, 'r', encoding='utf-8')
        #file = open("./"+filenm, 'r', encoding='utf-8')
        #f = csv.reader(file)
        '''
        excel_xy = pd.read_csv('C:/Users/user/Desktop/파이썬_test/기상청18_오픈API활용가이드_격자_위경도(20210106)_수정.csv'
                               ,sep=",",delimiter=",",error_bad_lines=False)
        print("위경도 가져오기", excel_xy)
        '''
        '''
        path = 'C:/Users/likey/Desktop/파이썬_테스트/2/기상청18_동네예보 조회서비스_오픈API활용가이드_격자_위경도(20210106).xlsx'
        with open(path, encoding='ansi', errors='ignore') as f:
            lines  = f.readlines()
            print("위경도 가져오기", lines)
        '''    
        '''
        for i in file:
            l = i.split(",")
            LCATEGORYNM.append(l[0])
            CATEGORYNO.append(l[1])
            CATEGORYNM.append(l[2])
            RANK.append(l[3])
            KEYWORD.append(l[4].replace('\n',''))
        file.close()
        return KEYWORD
        '''
        excel_data = pd.read_excel(r'C:/Users/Lucy/Desktop/파이썬_테스트/2/기상청18_동네예보 조회서비스_오픈API활용가이드_격자_위경도(20210106).xlsx', 
                                   sheet_name='최종업데이트파일_20210106', engine='openpyxl', skiprows='0')
        #print("============= 원본 엑셀 =============")
        #print(excel_data)
        
        
        return excel_data
        

    except Exception as e:
        print("[fileOpen] - ", e)


# 날씨 API 호출 -------------------------------------------------------------------------------------------------------------
def GetWeather(df_data, data, s_yyyymmdd):
    print("============= api로 호출해서 받은 데이터 ===============")
    print(data)
    
    empty_df = pd.DataFrame(index=range(0, len(data.index)), columns=['month ', 'day ', 'dayWeek ', 
                       '발표시각(baseDate) ','발표일자(baseTime) ','자료구분코드(category) ',
                       '예보지점 X좌표(nx) ','예보지점 Y좌표(ny) ','실황값(obsrValue) '])
  
    try:
        basis_dt = datetime.strptime(s_yyyymmdd, '%Y%m%d')
        #rows = [] # 리스트
        
        week = []
        month = []
        day = []
        dayWeek = []
        
        '''
        list_from_df = data.values.tolist()
        print(list_from_df)
        '''
        
        #print("===3====")
        dict_from_df = data.to_dict('list')
        #print(dict_from_df)        
      
        
        
        for i in range(0, len(data.index)):
            week.append(GetWeek(basis_dt))
            month.append(basis_dt.month)
            day.append(basis_dt.day)
            dayWeek.append(GetDayWeek(basis_dt))
           
        
        
        df = pd.DataFrame({"month ":month, "day ":day, "dayWeek ":dayWeek, 
                           "발표시각(baseDate) ":dict_from_df['baseDate'], 
                    "발표일자(baseTime) ":dict_from_df['baseTime'], 
                    "자료구분코드(category) ":dict_from_df['category'], 
                    "예보지점 X좌표(nx) ":dict_from_df['nx'], 
                    "예보지점 Y좌표(ny) ":dict_from_df['ny'], 
                    "실황값(obsrValue) ":dict_from_df['obsrValue']})
        #print("최종저장 컬럼: ", df.columns)
        print("최종저장 값: ", df_data.columns)
        
        #------------------
        for i in range(0, len(data.index)):
            empty_df.append(data.obsrValue)
        
        
        
        
        #------------------
        return df
    except Exception as e:
        print("[GetWeather()] - ", e)
        
        
# 파일이 생성되면 해당 파일을 SFTP로 목적지에 업로드한다.
def upload_file(vFilenm):
    try:
        host = uploadIP
        port = 22
        usr='dsadm'
        key_file = '/home/dbadmin/.ssh/id_rsa'
        tgt_path = dirPath+vFilenm
        src_path = filePath+vFilenm

        trans = paramiko.SSHClient()
        trans.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        key = paramiko.RSAKey.from_private_key_file(key_file)
        trans.connect(host, port=52180, username=usr, pkey=key)

        sftp = trans.open_sftp()
        sftp.put(src_path, tgt_path)

        sftp.close()
        trans.close()
    except Exception as e:
        print("[uploadFile()] - ", e)

# main -----------------------------------------------------------------------------------------------------------------
def main():
    try:
        "parameter setting: 시작일자/종료일자"
        if len(sys.argv) == 2:
            #s_date = str((datetime.strptime(sys.argv[1], "%Y%m%d")).date())
            s_yyyymmdd = sys.argv[1]
        else:
            #s_date = str((datetime.now() + timedelta(days=-1)).date())
            #s_yyyymmdd = ((datetime.now() + timedelta(days=-1)).date()).strftime('%Y%m%d')
            s_yyyymmdd = ((datetime.now()).strftime('%Y%m%d'))
            
        df_data = fileOpen()
        base_x = []
        base_y = []
        
        base_x = df_data.iloc[:, [5]].astype(int)
        base_y = df_data.iloc[:, [6]].astype(int)
        print("base_x", base_x )


        # 기상청 동네예보 데이터(초단기 실황) API 호출 
        #for i in range(0, len(base_x)):
        for i in range(0, 1):    
            # url for request 
            url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService/getUltraSrtNcst'    
            # parameter for request 
            params = '?' + urlencode({ 
                quote_plus('ServiceKey'): 'u%2Fx5iLeVtkcgbG1c41ezffIqNWA6Y9u%2FC7cEAeywZA%2FmrMO0QoLw5l8gUEmLeCzRttADYjVWYLfZ7YXWOFkXag%3D%3D',
                        # 공공데이터포털에서 받은 인증키     
            quote_plus('pageNo'): '1', # 페이지 번호(Default: 1)
            quote_plus('numOfRows'): '24', # 한 페이지 결과수(Default: 10)
            quote_plus('dataType'): 'JSON', # 요청자료형식(Default: XML)
            quote_plus('base_date'): s_yyyymmdd, # 발표일자(ex: 20151201)
            quote_plus('base_time'): '0600', # 발표시각
            quote_plus('nx'): df_data.iat[i, 5], # 예보지점 X 좌표
            quote_plus('ny'): df_data.iat[i, 6] # 예보지점 Y 좌표
            })

            req = urllib.request.Request(url + unquote(params))
            response_body = urlopen(req, timeout=60).read() # get bytes data 
            print("주소 인자 : ", unquote(params))
            

            json_data = json.loads(response_body)
            data = pd.DataFrame(json_data['response']['body']['items']['item'])
            print(data)
            print("==============================")
            print(type(data))
            print(data.shape)
            #print(data.loc[:, ['tm']])
        
        
            df = GetWeather(df_data, data, s_yyyymmdd)
            # 함수호출
            print("===최종====")
            print(df)
        
       
        
        filenm = 'weather_village_' + s_yyyymmdd + '.txt'
       
        df.to_csv(filePath+filenm, header=True, index=False, sep = ',', encoding='UTF-8', line_terminator='\n')

        #upload_file(filenm)
        
        
    except Exception as e:
        print("[main()] - ", e)


if __name__ == '__main__':
    main()