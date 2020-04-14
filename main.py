#!/usr/bin/python3
# -*- coding: UTF-8 -*-
  
import serial
import mysql.connector, sys
import os
import datetime as dt

### Текущие вопросы

# Не проходит сигнал о включении плинтуса на сайт
# Нет лога о включении плинтуса на сайт

############################################   Подпрограммы

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
    #f.write(text_info.decode("utf-8") + ' ') 
    #f.write(" -  ") 
    #f.write(d)
    #f.write(" - main.py")
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
    if int(result[0]) != status_plintus:
        p_id = last_line + 1      
        cursor.execute("""INSERT INTO plintus VALUES (%s,%s,%s)""",(p_id, d, status_plintus)) 
        cnx.commit()

def taps_p(taps_status_p):
    cursor.execute("SELECT * from taps_p")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
    result = cursor.fetchone()
    if int(result[0]) != taps_status_p:
        p_id = last_line + 1
        cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, taps_status_p))
        cnx.commit() 

def taps_t(taps_status_t):
    cursor.execute("SELECT * from taps_wc")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
    result = cursor.fetchone()
    if int(result[0]) != taps_status_t:
        p_id = last_line + 1
        cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, taps_status_t))
        cnx.commit() 

###################################    Processing


def data_processing(in_info):                    #   Обработка информации от МК     

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

    elif in_info == b'000003002':                #   Потеря связи с МКп
        ser.write(b'c')
        ser.write(b'032')
        logs(text_info, d, 'Потеря связи с МКп')
        event = "MKp turned off"
        MK = 'MKp'
        info_for_reboot_chart(MK, 'MK shutdown')                #   Записать инфу в таблицу reboot
        status_MK = 0
        MK_number = 3
        status_MK_update(status_MK, MK_number)    #   Запись статуса МКп в таблицу status_mk

    elif in_info == b'000003003':                 #   Отключение таймера повтора ?
        ser.write(b'c')
        ser.write(b'033')
        #logs(text_info, d, 'Потеря связи с МКп')

    elif in_info == b'000003005':                 #   Сигнал включения плинтуса
        ser.write(b'c')
        ser.write(b'035')
        logs(text_info, d, 'Включение плинтуса')
        plintus(1)

    elif in_info == b'000003006':                 #   Сигнал выключения плинтуса по таймеру
        ser.write(b'c')
        ser.write(b'036')
        logs(text_info, d, 'Выключение плинтуса по таймеру')
        plintus(0)

    elif in_info == b'000003007':                 #   Сигнал выключения
        ser.write(b'c')
        ser.write(b'037')
        logs(text_info, d, 'Выключение воды')
        taps_p(0)

    elif in_info == b'000003008':                 #   Сигнал включения воды
        ser.write(b'c')
        ser.write(b'038')
        logs(text_info, d, 'Включение воды')
        taps_p(1)

    elif in_info == b'000003009':                 #   Выключение воды, сработал датчик
        ser.write(b'c')
        ser.write(b'039')
        logs(text_info, d, 'Выключение воды, сработал датчик')
        taps_p(0)

    elif in_info == b'000004000':                #   Инфо о перезагрузке МКт     
        ser.write(b'c')
        ser.write(b'040')
        logs(text_info, d, 'Перезагрузка МКт')
        MK = 'MKt'
        info_for_reboot_chart(MK, 'Reboot of MK')                #   Записать инфу в таблицу reboot
        status_MK = 1
        MK_number = 4
        status_MK_update(status_MK, MK_number)   # Запись статуса МКт в таблицу

    elif in_info == b'000004002':                #   Потеря связи с МКт 
        ser.write(b'c')
        ser.write(b'042')
        logs(text_info, d, 'Потеря связи с МКт')
        MK = 'MKt'
        info_for_reboot_chart(MK, 'MK shutdown')                #   Записать инфу в таблицу reboot
        status_MK = 0
        MK_number = 4
        status_MK_update(status_MK, MK_number)   # Запись статуса МКт в таблицу

    elif in_info == b'001004005':                #   Сигнал от датчика 1 Кухня, МКт
        ser.write(b'c')
        ser.write(b'145')
        logs(text_info, d, 'Сигнал от датчика 1 Кухня, МКт')
        taps_t(0)
        Sensor1_wc = """UPDATE `detectors_wc` SET d1 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor1_wc,datas)
        cnx.commit()

    elif in_info == b'001004006':                #   Сигнал от датчика 1 Ниша, МКт
        ser.write(b'c')
        ser.write(b'146')
        logs(text_info, d, 'Сигнал от датчика 1 Ниша, МКт')
        taps_t(0)
        Sensor2_wc = """UPDATE `detectors_wc` SET d2 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor2_wc,datas)
        cnx.commit()

    elif in_info == b'001004007':                #   Сигнал от датчика 3 МКт Туалет
        ser.write(b'c')
        ser.write(b'147')
        logs(text_info, d, 'Сигнал от датчика 1 Туалет, МКт')
        taps_t(0)
        Sensor3_wc = """UPDATE `detectors_wc` SET d3 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor3_wc,datas)
        cnx.commit()

    elif in_info == b'001004008':                #   Сигнал от кнопки включения воды на МКт
        ser.write(b'c')
        ser.write(b'148')
        logs(text_info, d, 'Сигнал от кнопки включения воды на МКт, МКт')
        taps_t(1)
        Sensor1_wc = """UPDATE `detectors_wc` SET d1 = %s WHERE p_id = %s"""
        datas = (0, 1)    
        cursor.execute(Sensor1_wc,datas)
        cnx.commit()
        Sensor2_wc = """UPDATE `detectors_wc` SET d2 = %s WHERE p_id = %s"""
        datas = (0, 1)    
        cursor.execute(Sensor2_wc,datas)
        cnx.commit()
        Sensor3_wc = """UPDATE `detectors_wc` SET d3 = %s WHERE p_id = %s"""
        datas = (0, 1)    
        cursor.execute(Sensor3_wc,datas)
        cnx.commit()

    elif in_info == b'001004009':                #   Сигнал от кнопки выключения воды на МКт
        ser.write(b'c')
        ser.write(b'149')
        logs(text_info, d, 'Сигнал от кнопки выключения воды на МКт, МКт')
        taps_t(0)


#############################   Main

ser = serial.Serial("/dev/ttyUSB0", 19200, timeout=2, writeTimeout=1)
ser.write(b'0')
try:
    rb1 = ser.read(3)
    rb2 = ser.read(3)
    rb3 = ser.read(3)
    text_info = rb1 + rb2 + rb3
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


#text_info = b'000003008'

data_processing(text_info)                     #   Обработать входящую информацию




cursor.close()
cnx.close()
ser.close()



