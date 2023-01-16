import pandas as pd
import requests, json
from datetime import datetime
import pytz
import re
import mysql.connector
import time

tz = pytz.timezone('Asia/Bangkok')
d = datetime.now(tz).strftime('%Y-%m-%d')
def check_datenow(t):
    return d in t

mydb = mysql.connector.connect(
      host="localhost",
      user="root",
      password="",
      database="db_pollution"
    )
mycursor = mydb.cursor() 
while True:
    data = []
    url3 = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"
    r3 = requests.get(url3)
    temp_data = dict(r3.json())
    for i in temp_data['stations']:
        l_temp=i['AQILast']
        sql = "SELECT * FROM pollution_data WHERE area=%s AND time=%s AND date=%s"
        adr = (i['areaTH'],l_temp['time'],l_temp['date'])
        mycursor.execute(sql, adr)
        myresult = mycursor.fetchall()
        if len(myresult) == 0:
            d_temp = {}
            d_temp['Lat'] = i['lat']
            d_temp['Lng'] = i['long']
            l_temp=i['AQILast']
            d_temp['aqi'] = l_temp['AQI']['aqi']
            if 'PM25'  in list(l_temp.keys()):
                pm25=l_temp['PM25']['value']
                if pm25 != 'n/a' and pm25!='' and pm25!='-' and pm25!='N/A':
                    d_temp['pm2.5'] = pm25
                else:
                    d_temp['pm2.5'] = ''
            else:
                d_temp['pm2.5'] = ''
            if 'PM10'  in list(l_temp.keys()):
                pm10=l_temp['PM10']['value']
                if pm10 != 'n/a' and pm10!='' and pm10!='-' and pm10!='N/A':
                    d_temp['pm10'] = pm10
            
            d_temp['area'] = i['areaTH']
            d_temp['time'] = l_temp['time']
            d_temp['date'] = l_temp['date']
            d_temp['source'] = "air4thai"
            txt = d_temp['area']
            x1 = re.search(".*กรุงเทพฯ$",txt)
            x2 = re.search(".*กระบี่$",txt)
            x3= re.search(".*กาญจนบุรี$",txt)
            x4 = re.search(".*กาฬสินธุ์$",txt)
            x5 = re.search(".*กำแพงเพชร$",txt)
            x6= re.search(".*ขอนแก่น$",txt)
            x7 = re.search(".*จันทบุรี$",txt)
            x8 = re.search(".*ฉะเชิงเทรา$",txt)
            x9 = re.search(".*ชลบุรี$",txt)
            x10 = re.search(".*ชัยนาท$",txt)
            x11 = re.search(".*ชัยภูมิ$",txt)
            x12 = re.search(".*ชุมพร$",txt)
            x13 = re.search(".*เชียงราย$",txt)
            x14 = re.search(".*เชียงใหม่$",txt)
            x15 = re.search(".*ตรัง$",txt)
            x16 = re.search(".*ตราด$",txt)
            x17 = re.search(".*ตาก$",txt)
            x18 = re.search(".*นครนายก$",txt)
            x19 = re.search(".*นครปฐม$",txt)
            x20 = re.search(".*นครพนม$",txt)
            x21 = re.search(".*นครราชสีมา$",txt)
            x22 = re.search(".*นครสวรรค์$",txt)
            x23 = re.search(".*นนทบุรี$",txt)
            x24 = re.search(".*นราธิวาส$",txt)
            x25 = re.search(".*น่าน$",txt)
            x26 = re.search(".*บึงกาฬ$",txt)
            x27 = re.search(".*บุรีรัมย์$",txt)
            x28 = re.search(".*ปทุมธานี$",txt)
            x29 = re.search(".*ประจวบคิรีขันธ์$",txt)
            x30 = re.search(".*ปราจีนบุรี$",txt)
            x31 = re.search(".*ปัตตานี$",txt)
            x32 = re.search(".*พระนครศรีอยุธยา$",txt)
            x33 = re.search(".*พะเยา$",txt)
            x34 = re.search(".*พังงา$",txt)
            x35 = re.search(".*พัทลุง$",txt)
            x36 = re.search(".*พิจิตร$",txt)
            x37 = re.search(".*พิษณุโลก$",txt)
            x38 = re.search(".*เพชรบุรี$",txt)
            x39 = re.search(".*เพชรบูรณ์$",txt)
            x40 = re.search(".*แพร่$",txt)
            x41 = re.search(".*ภูเก็ต$",txt)
            x42 = re.search(".*มหาสารคาม$",txt)
            x43 = re.search(".*มุกดาหาร$",txt)
            x44 = re.search(".*แม่ฮ่องสอน$",txt)
            x45 = re.search(".*ยโสธร$",txt)
            x46 = re.search(".*ยะลา$",txt)
            x47 = re.search(".*ร้อยเอ็ด$",txt)
            x48 = re.search(".*ระนอง$",txt)
            x49 = re.search(".*ระยอง$",txt)
            x50 = re.search(".*ราชบุรี$",txt)
            x51 = re.search(".*ลพบุรี$",txt)
            x52 = re.search(".*ลำปาง$",txt)
            x53 = re.search(".*ลำพูน$",txt)
            x54 = re.search(".*เลย$",txt)
            x55 = re.search(".*ศรีสะเกษ$",txt)
            x56 = re.search(".*สกลนคร$",txt)
            x57 = re.search(".*สงขลา$",txt)
            x58 = re.search(".*สตูล$",txt)
            x59 = re.search(".*สมุทรปราการ$",txt)
            x60 = re.search(".*สมุทรสงคราม$",txt)
            x61 = re.search(".*สมุทรสาคร$",txt)
            x62 = re.search(".*สระแก้ว$",txt)
            x63 = re.search(".*สระบุรี$",txt)
            x64 = re.search(".*สิงห์บุรี$",txt)
            x65 = re.search(".*สุโขทัย$",txt)
            x66 = re.search(".*สุพรรณบุรี$",txt)
            x67 = re.search(".*สุราษฎร์ธานี$",txt)
            x68 = re.search(".*สุรินทร์$",txt)
            x69 = re.search(".*หนองคาย$",txt)
            x70 = re.search(".*หนองบัวลำภู$",txt)
            x71 = re.search(".*อ่างทอง$",txt)
            x72 = re.search(".*อำนาจเจริญ$",txt)
            x73 = re.search(".*อุดรธานี$",txt)
            x74 = re.search(".*อุตรดิตถ์$",txt)
            x75 = re.search(".*อุทัยธานี$",txt)
            x76 = re.search(".*อุบลราชธานี$",txt)
            x77 = re.search(".*นครศรีธรรมราช",txt)
            if (x1) or (x6) or (x9) or (x10) or (x13) or (x14) or (x18) or (x21) or (x23) or (x28) or (x47) or (x49) or (x57) or (x71) or (x73) or (x76):
                d_temp['layer'] = "การคมนาคมและขนส่ง"
            if (x4) or (x5) or (x7) or (x8) or (x11) or (x12) or (x16) or (x17) or (x25) or (x26) or (x32) or (x33) or (x35) or (x37) or (x38) or (x39) or (x40) or (x43) or(x44)or(x45)or(x48)or(x52)or(x53)or(x54)or(x55)or(x56)or(x62)or(x66)or(x67)or(x69)or(x72)or(x77):
                d_temp['layer'] = "เผาในที่โล่ง"
            if (x2)or(x3)or(x15)or(x19)or(x22)or(x27)or(x29)or(x30)or(x31)or(x36)or(x42)or(x50)or(x51)or(x59)or(x60)or(x61)or(x63)or(x64)or(x65)or(x68)or(x70)or(x74)or(x75):
                d_temp['layer'] = "ภาคอุตสาหกรรม"
            if (x20)or(x41):
                d_temp['layer'] = "การก่อสร้าง"
            if (x50)or(x24)or(x46)or(x58):
                d_temp['layer'] = "หมอกควันข้ามแดน"

            if check_datenow(d_temp['date']):
                data.append(d_temp)

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
    import xmltodict
    data_bangkok=dict(json.loads(json.dumps(xmltodict.parse(r2.text),ensure_ascii=False)))
    for i in data_bangkok['markers']['marker']:
        print(12)
        sql = "SELECT * FROM pollution_data WHERE area=%s AND time=%s AND date=%s"
        # print(i['@name_th'])
        adr = (i['@name_th'].strip() +i['@district_th'].strip()+ " กรุงเทพฯ",month(i['@date_time']),td(i['@time']))
        mycursor.execute(sql, adr)
        myresult = mycursor.fetchall()
        if len(myresult) == 0:

            d_temp = {}
            d_temp['Lat'] = i['@lat']
            d_temp['Lng'] = i['@lng']
            d_temp['aqi'] = ''
            if '@pm25' in list(i.keys()):
                if i['@pm25'] != 'n/a' and i['@pm25']!='' and i['@pm25']!='-':
                    d_temp['pm2.5'] = i['@pm25']
                else:
                    d_temp['pm2.5'] = ''
            else:
                d_temp['pm2.5'] =''
            if '@pm10' in list(i.keys()):
                d_temp['pm10'] = i['@pm10']
            d_temp['area'] = i['@name_th'].strip() +i['@district_th'].strip()+ " กรุงเทพฯ"
            print(i['@name_th'].strip() +i['@district_th'].strip()+ " กรุงเทพฯ")
            d_temp['date'] = month(i['@date_time'])
            d_temp['time'] = td(i['@time'])
            d_temp['source'] = "bangkokairquality"
            d_temp['layer'] = "การคมนาคมและขนส่ง"
            if check_datenow(d_temp['date']):
                data.append(d_temp)

    df=pd.DataFrame(data)
    df
    from datetime import datetime
    print(f"Hello @ {datetime.now()}")
    all_data = []
    for i in range(len(data)):
        data2= "'"+str(df["Lat"][i])+"','"+str(df["Lng"][i])+"','"+str(df["aqi"][i])+"','"+str(df["pm2.5"][i])+"','"+str(df["pm10"][i])+"','"+str(df["area"][i])+"','"+str(df["time"][i])+"','"+str(df["date"][i])+"','"+str(df["source"][i])+"','"+str(df["layer"][i])+"'"
        print(data2)
        all_data.append(data2)

    mycursor = mydb.cursor() 
    for d in all_data:
        query = "insert into pollution_data(lat,lng,aqi,pm25,pm10,area,time,date,source,layer) values ({})".format(d)
        print(query)
        mycursor.execute(query)
        mydb.commit()
    time.sleep(3600)
    