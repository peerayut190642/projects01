import pandas as pd
import requests, json
from datetime import datetime
import pytz
import re
import mysql.connector


tz = pytz.timezone('Asia/Bangkok')

mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="",
      database="db_pollution"
    )
mycursor = mydb.cursor() 
mycursor.execute("Show tables;")
table = mycursor.fetchall()
def month(str1):
    list_month=['มกราคม' , 'กุมภาพันธ์' , 'มีนาคม' , 'เมษายน' , 'พฤษภาคม' , 'มิถุนายน' , 'กรกฎาคม' , 'สิงหาคม' , 'กันยายน' , 'ตุลาคม' , 'พฤศจิกายน' , 'ธันวาคม']
    list_l=str1.split()
    return list_l[2]+"-"+"{:02d}".format(list_month.index(list_l[1])+1)+"-"+"{:02d}".format(int(list_l[0]))
def td(str):
    list_l=str.split()
    return list_l[0]
url2 = "https://bangkokairquality.com/bma/marker.php"
r2 = requests.get(url2)
d2 = datetime.now(tz).strftime('%d-%m-%Y')
def check_datenow2(t):
  return d2 in t
import xmltodict
data_bangkok=dict(json.loads(json.dumps(xmltodict.parse(r2.text),ensure_ascii=False)))
for i in data_bangkok['markers']['marker']:
    sql = "SELECT * FROM pollution_data WHERE area=%s AND time=%s AND date=%s"
    adr = (i['@name_th'].strip() +i['@district_th'].strip()+ " กรุงเทพฯ",month(i['@date_time']),td(i['@time']))
    mycursor.execute(sql, adr)
    myresult = mycursor.fetchall()
    if len(myresult) == 0:
        mycursor.execute(sql, adr)
        myresult = mycursor.fetchall()
        if len(myresult) == 0:
            print("ssss")
        else:
            for x in myresult:
                print(x)