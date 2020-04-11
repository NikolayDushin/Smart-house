#!/usr/bin/python3
# -*- coding: UTF-8 -*-

# Отработка прохождения сигнала включения плинтуса через сайт 0,3,5 и выключения 0,3,6
  
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

def error_log(error_name, error_description):   #   подпрограмма записи логов ошибок
    current_time = dt.datetime.utcnow() + dt.timedelta(hours=3)    # текущее время в Москве
    current_time = current_time.strftime("%d-%m-%Y %H:%M")
    full_log_line = str(current_time) + ' - ' + 'main.py' + ' - ' + str(error_name) + ' - ' + str(error_description) 
    f = open("/home/pi/sh/python/logs/errors.log", "a")   
    f.write(full_log_line)
    f.write("  \n") 
    f.close()


def logs(text_info, d, log_info):                   #   Запись логов
    f = open("/home/pi/sh/python/logs/main_log.txt", "a")
    f.write(text_info.decode("utf-8") + '  -  ' + d + '  -  main.py  -  ' + log_info)
    f.write("\n")
    f.close()

def info_for_reboot_chart(MK, event):                   #   Записать инфо о перезагрузке в таблицу
    cursor.execute("SELECT * from reboot")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    r_id = last_line + 1
    #event = "Reboot of MK"
    cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
    cnx.commit()

def status_MK_update(status_MK, number_MK):
    Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
    datas = (status_MK, number_MK)    
    cursor.execute(Status_MKk,datas)
    cnx.commit()

def plintus(status_plintus):
    cursor.execute("SELECT * from plintus")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    cursor.execute("SELECT p_status from plintus WHERE p_id = %s"%(last_line))
    result = cursor.fetchone() 
    if result[0] == "1":
        p_id = last_line + 1      
        cursor.execute("""INSERT INTO plintus VALUES (%s,%s,%s)""",(p_id, d, status_plintus)) 
        cnx.commit()


def data_processing(in_info):                    #   Обработка информации от МК    print(in_info)


    print(in_info)


    if in_info == b'000000000':
        ser.close()
        exit()


    elif in_info == b'000001000':                #   Инфо о перезагрузке МКк
        ser.write(b'c')                          #   Отключение цикла опроса на МКк
        ser.write(b'010')
        logs(text_info, d, 'Перезагрузка МКк')                       #   Записать логи
        MK = 'MKk'                                       
        info_for_reboot_chart(MK, 'Reboot of MK')                #   Записать инфу в таблицу reboot
        ser.write(b'D')
        ser.write(d1.encode("utf-8"))
        status_MK = 1
        MK_number = 1
        status_MK_update(status_MK, MK_number)    # Запись статуса МКк в таблицу 
        status_MK = 0
        MK_number = 3
        status_MK_update(status_MK, MK_number)    # Запись статуса МКп в таблицу
        status_MK = 0
        MK_number = 4
        status_MK_update(status_MK, MK_number)    # Запись статуса МКт в таблицу
        status_MK = 0
        MK_number = 5
        status_MK_update(status_MK, MK_number)    # Запись статуса МКг в таблицу


    elif in_info == b'000003000':                #   Инфо о перезагрузке МКп
        ser.write(b'c')
        ser.write(b'030')
        logs(text_info, d, 'Перезагрузка МКп')
        MK = 'MKp'
        info_for_reboot_chart(MK, 'Reboot of MK')                #   Записать инфу в таблицу reboot
        status_MK = 1
        MK_number = 3
        status_MK_update(status_MK, MK_number)    # Запись статуса МКп в таблицу status_mk


    elif in_info == b'000003005':                 #   Сигнал включения плинтуса
        ser.write(b'c')
        ser.write('035')
        logs(text_info, d, 'Включение плинтуса')
        plintus(1)

    elif in_info == b'000003006':                 #   Сигнал выключения плинтуса по таймеру
        ser.write(b'c')
        ser.write('036')
        logs(text_info, d, 'Выключение плинтуса по таймеру')
        plintus(0)


    elif in_info == b'000004000':                #   Инфо о перезагрузке МКт     
        ser.write(b'c')
        ser.write(b'040')
        logs(text_info, d, 'Перезагрузка МКт')
        MK = 'MKt'
        info_for_reboot_chart(MK, 'Reboot of MK')                #   Записать инфу в таблицу reboot
        status_MK = 1
        MK_number = 4
        status_MK_update(status_MK, MK_number)   # Запись статуса МКт в таблицу






#####################################################

ser = serial.Serial("/dev/ttyUSB0", 19200, timeout=2, writeTimeout=1)

ser.write(b"0")

try:
    rb1 = ser.read(3)
    rb2 = ser.read(3)
    rb3 = ser.read(3)
    text_info = rb1 + rb2 + rb1
    print ("G status:",  rb1)
    print ("G status:",  rb2)
    print ("G status:",  rb3)
    rb1 = int(rb1)
    rb2 = int(rb2)
    rb3 = int(rb3)

except ValueError:
    print(ValueError)
    error_log('<ValueError>','Нет связи с МКк')
    ser.close()
    exit()

user_value, db_value, pw_value = keys()          #   Cчитать пароль к базе данных
cnx = mysql.connector.connect(user = user_value, database = db_value, password = pw_value)
cursor = cnx.cursor()

current_time = dt.datetime.utcnow() + dt.timedelta(hours=3)   #   Текущее время
d1 = current_time.strftime("%S%M%H%d%m%y%w")
d = current_time.strftime("%Y-%m-%d %H:%M:%S")

data_processing(text_info)                     #   Обработать входящую информацию















#cursor.execute("SELECT * from taps_p")
#rows = cursor.fetchall()
#last_line = cursor.rowcount

#cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
#result = cursor.fetchone()

#print(result[0])

#if result[0] != 1: print(777)



cursor.close()
cnx.close()
ser.close()

