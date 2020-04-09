#!/usr/bin/python3
# -*- coding: UTF-8 -*-
  
import serial
import mysql.connector, sys
import os
import datetime as dt

def keys():                                # Получить ключи к базе данных
    try:
        f = open("/home/pi/sh/keys/keys.txt",'r',encoding = 'utf-8')
    except FileNotFoundError:
        print("<File keys.txt not found>")
        error_log("FileNotFoundError","File keys.txt not found")     #   записать лог ошибки
        exit()
    user_value = f.readline()
    user_value= user_value.rstrip()
    db_value = f.readline()
    db_value = db_value.rstrip()
    pw_value = f.readline()
    pw_value = pw_value.rstrip()
    return user_value, db_value, pw_value




user_value, db_value, pw_value = keys()          #   Cчитать пароль к базе данных
cnx = mysql.connector.connect(user = user_value, database = db_value, password = pw_value)
cursor = cnx.cursor()

current_time = dt.datetime.utcnow() + dt.timedelta(hours=3)   #   Текущее время
d1 = current_time.strftime("%S%M%H%d%m%y%w")
d = current_time.strftime("%Y-%m-%d %H:%M:%S")




cursor.execute("SELECT * from taps_p")
rows = cursor.fetchall()
last_line = cursor.rowcount

cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
result = cursor.fetchone()

print(result[0])

if result[0] != 1: print(777)


#if result is None:
#    p_id = last_line + 1
#    cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 1))
#    cnx.commit() 
#else:
#   if result[0] == "0":
#       p_id = last_line + 1      
#       cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 1)) 
#       cnx.commit()

