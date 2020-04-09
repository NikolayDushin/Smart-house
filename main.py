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
    if result[0] == "1":
        p_id = last_line + 1      
        cursor.execute("""INSERT INTO plintus VALUES (%s,%s,%s)""",(p_id, d, status_plintus)) 
        cnx.commit()

def taps_p(taps_status):
    cursor.execute("SELECT * from taps_p")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
    result = cursor.fetchone()
    if result != taps_status:
        p_id = last_line + 1
        cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, taps_status))
        cnx.commit() 

def taps_t(taps_status):
    cursor.execute("SELECT * from taps_wc")
    rows = cursor.fetchall()
    last_line = cursor.rowcount
    cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
    result = cursor.fetchone()
    if result != taps_status:
        p_id = last_line + 1
        cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0))
        cnx.commit() 














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
        info_for_reboot_chart(MK)                #   Записать инфу в таблицу reboot
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
        #ser.write(b'c')
        #ser.write(b'030')
        logs(text_info, d, 'Перезагрузка МКп')
        MK = 'MKp'
        info_for_reboot_chart(MK, 'Reboot of MK')                #   Записать инфу в таблицу reboot
        status_MK = 1
        MK_number = 3
        status_MK_update(status_MK, MK_number)    # Запись статуса МКп в таблицу status_mk

    elif in_info == b'000003002':                #   Потеря связи с МКп
        #ser.write(b'c')
        #ser.write(b'032')
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
        #ser.write(b'c')
        #ser.write('035')
        logs(text_info, d, 'Включение плинтуса')
        plintus(1)

    elif in_info == b'000003006':                 #   Сигнал выключения плинтуса по таймеру
        #ser.write(b'c')
        #ser.write('036')
        logs(text_info, d, 'Выключение плинтуса по таймеру')
        plintus(0)

    elif in_info == b'000003007':                 #   Сигнал выключения воды кнопкой или через сайт
        #ser.write(b'c')
        #ser.write(b'037')
        logs(text_info, d, 'Выключение воды по кнопкой или через сайт')
        taps_p(0)

    elif in_info == b'000003008':                 #   Сигнал включения воды кнопкой или через сайт
        #ser.write(b'c')
        #ser.write(b'038')
        logs(text_info, d, 'Включение воды по кнопкой или через сайт')
        taps_p(1)

    elif in_info == b'000003009':                 #   Выключение воды, сработал датчик
        #ser.write(b'c')
        #ser.write(b'039')
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
        #ser.write(b'c')
        #ser.write('042')
        logs(text_info, d, 'Потеря связи с МКт')
        MK = 'MKt'
        info_for_reboot_chart(MK, 'MK shutdown')                #   Записать инфу в таблицу reboot
        status_MK = 0
        MK_number = 4
        status_MK_update(status_MK, MK_number)   # Запись статуса МКт в таблицу

    elif in_info == b'000004005':                #   Сигнал от датчика 1 Кухня, МКт
        #ser.write(b'c')
        #ser.write(b'145')
        logs(text_info, d, 'Сигнал от датчика 1 Кухня, МКт')
        taps_t(0)
        Sensor1_wc = """UPDATE `detectors_wc` SET d1 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor1_wc,datas)
        cnx.commit()

    elif in_info == b'000004006':                #   Сигнал от датчика 1 Ниша, МКт
        #ser.write(b'c')
        #ser.write(b'146')
        logs(text_info, d, 'Сигнал от датчика 1 Ниша, МКт')
        taps_t(0)
        Sensor2_wc = """UPDATE `detectors_wc` SET d2 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor2_wc,datas)
        cnx.commit()

    elif in_info == b'000004007':                #   Сигнал от датчика 3 МКт Туалет
        #ser.write(b'c')
        #ser.write(b'147')
        logs(text_info, d, 'Сигнал от датчика 1 Туалет, МКт')
        taps_t(0)
        Sensor3_wc = """UPDATE `detectors_wc` SET d3 = %s WHERE p_id = %s"""
        datas = (1, 1)    
        cursor.execute(Sensor3_wc,datas)
        cnx.commit()













#############################   Main

ser = serial.Serial("/dev/ttyUSB0", 19200, timeout=2, writeTimeout=1)
ser.write(b'0')
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


text_info = b'000004007'

data_processing(text_info)                     #   Обработать входящую информацию






#############################   OLD

if int(rb1) == 0:                          #   Перезагрузка МКк
   if int(rb2) == 1 and int(rb3) == 10 :
      ser.write(b'c')                      #   Отключение цикла опроса на МКк
      ser.write(b'010')
      LogON = 1                                 #   Записать логи  
      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line + 1
      event = "Reboot of MK"
      MK = int(rb1)
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()

      current_time = dt.datetime.utcnow() + dt.timedelta(hours=3)   #   Обновление времени на МКк
      print(current_time.strftime("%S%M%H%d%m%y%w"))

      ser.write(b'D')
      ser.write(d1.encode("utf-8"))

      Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (1, 1)    
      cursor.execute(Status_MKk,datas)
      cnx.commit()

      ###   При выключении МКк, статус МКп - выключен   #####################      
      Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (0, 3)    
      cursor.execute(Status_MKk,datas)
      cnx.commit()

      ###   При выключении МКк, статус МКт - выключен   #####################      
      Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (0, 4)    
      cursor.execute(Status_MKk,datas)
      cnx.commit()

      ###   При выключении МКк, статус МКг - выключен   #####################      
      Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (0, 5)    
      cursor.execute(Status_MKk,datas)
      cnx.commit()





if int(rb1) == 0:                          #   Перезагрузка МКп 
   if int(rb2) == 3 and int(rb3) == 10 :
      ser.write(b'c')
      ser.write(b'030')
      LogON = 1
      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line+1
      event="Reboot of MK"
      MK = int(rb2)
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()
      Status_MKk = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (1, 3)    
      cursor.execute(Status_MKk,datas)
      cnx.commit()




if int(rb1) == 0:                              #   Сигнал выключения МКп  
   if int(rb2) == 3 and int(rb3) == 20 :
      ser.write(b'c')
      ser.write(b'032')
      LogON = 1
      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line+1
      event = "MKp turned off"
      MK = 3
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()
      Status_MKp = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (0, 3)    
      cursor.execute(Status_MKp,datas)
      cnx.commit()

if int(rb1) == 0:
   if int(rb2) == 3 and int(rb3) == 30:
      ser.write(b'c')
      ser.write(b'033')
      LogON = 1

if int(rb1) == 0:                               #   Сигнал включения плинтуса
   if int(rb2) == 3 and int(rb3) == 50:
      ser.write(b'c')
      ser.write('035')
      LogON = 1
      cursor.execute("SELECT * from plintus")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      cursor.execute("SELECT p_status from plintus WHERE p_id = %s"%(last_line))
      result = cursor.fetchone() 
      if result[0] == "0":
         p_id = last_line + 1      
         cursor.execute("""INSERT INTO plintus VALUES (%s,%s,%s)""",(p_id, d, 1)) 
         cnx.commit()

if int(rb1) == 0:                               #   Сигнал выключения плинтуса 
   if int(rb2) == 3 and int(rb3) == 60:
      ser.write(b'c')
      ser.write(b'036')
      LogON = 1
      cursor.execute("SELECT * from plintus")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      cursor.execute("SELECT p_status from plintus WHERE p_id = %s"%(last_line))
      result = cursor.fetchone() 
      if result[0] == "1":
         p_id = last_line + 1      
         cursor.execute("""INSERT INTO plintus VALUES (%s,%s,%s)""",(p_id, d, 0)) 
         cnx.commit()


   if int(rb2) == 3 and int(rb3) == 70:                  #   Сигнал выключения воды на МКп
      ser.write(b'c')
      ser.write(b'037')
      LogON = 1

      cursor.execute("SELECT * from taps_p")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line+1
          cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line+1      
              cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit()

   if int(rb2) == 3 and int(rb3) == 80:                 #   Сигнал включения воды на МКп
      ser.write(b'c')
      ser.write(b'038')
      LogON = 1

      cursor.execute("SELECT * from taps_p")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 1))
          cnx.commit() 
      else:
          if result[0] == "0":
              p_id = last_line + 1      
              cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 1)) 
              cnx.commit()

if int(rb1) == 0:                                 #   Сигнал выключения воды на МКп
   if int(rb2) == 3 and int(rb3) == 90:
      ser.write(b'c')
      ser.write(b'039')
      LogON = 1
      cursor.execute("SELECT * from taps_p")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_p_status from taps_p WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line + 1      
              cursor.execute("""INSERT INTO taps_p VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit()


if int(rb1) == 0:                                      #   Перезагрузка МКт
   if int(rb2) == 4 and int(rb3) == 10:
      ser.write(b'c')
      ser.write(b'040')
      LogON = 1
      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line + 1
      event="Reboot of MK"
      MK = int(rb2)
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()
      Status_MKt = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas=(1, 4)    
      cursor.execute(Status_MKt,datas)
      cnx.commit()


#cursor.close()
#cnx.close()
#ser.close()
#exit()




if int(rb1) == 0:                                     #   Сигнал выключения МКт
   if int(rb2) == 4 and int(rb3) == 20:
      ser.write(b'c')
      ser.write('042')
      LogON = 1

      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line + 1

      event = "Device turned off"
      MK = 4
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()

      Status_MKt = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas = (0, 4)    
      cursor.execute(Status_MKt,datas)
      cnx.commit()

if int(rb1) == 1:                                    #   Сигнал от датчика 1 МКт КУХНЯ
   if int(rb2) == 4 and int(rb3) == 50:
      ser.write(b'c')
      ser.write(b'145')
      LogON = 1

      cursor.execute("SELECT * from taps_wc")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line+1      
              cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit() 

      Sensor1_wc = """UPDATE `detectors_wc` SET d1 = %s WHERE p_id = %s"""
      datas = (1, 1)    
      cursor.execute(Sensor1_wc,datas)
      cnx.commit()

if int(rb1) == 1:                                  #   Сигнал от датчика 2 МКт НИША
   if int(rb2) == 4 and int(rb3) == 60:
      ser.write(b'c')
      ser.write(b'146')
      LogON = 1

      cursor.execute("SELECT * from taps_wc")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line + 1      
              cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit() 

      Sensor2_wc = """UPDATE `detectors_wc` SET d2 = %s WHERE p_id = %s"""
      datas = (1, 1)    
      cursor.execute(Sensor2_wc,datas)
      cnx.commit()


if int(rb1) == 1:                                      #   Сигнал от датчика 3 МКт Туалет
   if int(rb2) == 4 and int(rb3) == 7:
      ser.write(b'c')
      ser.write(b'147')
      LogON = 1

      cursor.execute("SELECT * from taps_wc")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line + 1      
              cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit() 

      Sensor3_wc = """UPDATE `detectors_wc` SET d3 = %s WHERE p_id = %s"""
      datas = (1, 1)    
      cursor.execute(Sensor3_wc,datas)
      cnx.commit()

   if int(rb2) == 4 and int(rb3) == 8:                      #   Сигнал от кнопки включения воды на МКт
      ser.write(b'c')
      ser.write(b'148')
      LogON = 1

      cursor.execute("SELECT * from taps_wc")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line + 1
          cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 1))
          cnx.commit() 
      else:
          if result[0] == "0":
              p_id = last_line + 1      
              cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 1)) 
              cnx.commit() 

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

   if int(rb2) == 4 and int(rb3) == 9:                          #   Сигнал от кнопки выключения воды на МКт
      ser.write(b'c')
      ser.write(b'149')
      LogON = 1

      cursor.execute("SELECT * from taps_wc")
      rows = cursor.fetchall()
      last_line = cursor.rowcount

      cursor.execute("SELECT taps_wc_status from taps_wc WHERE p_id = %s"%(last_line))
      result = cursor.fetchone()
      if result is None:
          p_id = last_line+1
          cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0))
          cnx.commit() 
      else:
          if result[0] == "1":
              p_id = last_line+1      
              cursor.execute("""INSERT INTO taps_wc VALUES (%s,%s,%s)""",(p_id, d, 0)) 
              cnx.commit()




#cursor.close()
#cnx.close()
#ser.close()
#exit()









if int(rb1) == 0:                                       #    Перезагрузка МКг
   if int(rb2) == 5 and int(rb3) == 0:
      ser.write(b'c')
      ser.write(b'050')
      LogON = 1
 
      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line + 1
      event="Reboot of MK"
      MK = int(rb2)
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()
      Status_MKg = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas=(1, 5)    
      cursor.execute(Status_MKg,datas)
      cnx.commit()

if int(rb1) == 0:                                          #   Сигнал выключения МКг
   if int(rb2) == 5 and int(rb3) == 2:
      ser.write(b'c')
      ser.write(b'052')
      LogON = 1

      cursor.execute("SELECT * from reboot")
      rows = cursor.fetchall()
      last_line = cursor.rowcount
      r_id = last_line + 1

      event="Device turned off"
      MK = 5
      cursor.execute("""INSERT INTO reboot VALUES (%s,%s,%s,%s)""",(r_id, d, MK, event)) 
      cnx.commit()

      Status_MKt = """UPDATE `status_mk` SET w_s=%s WHERE w_s_id=%s"""
      datas=(0, 5)    
      cursor.execute(Status_MKt,datas)
      cnx.commit()


#if LogON == 1:
#    f = open("/home/pi/sh/python/logs/main_log.txt", "a")
#    f.write(rb1.decode("utf-8") + ' ')
#    f.write(rb2.decode("utf-8")+ ' ')
#    f.write(rb3.decode("utf-8")+ ' ')
#    f.write(" -  ") 
#    f.write(d)
#    f.write(" - main.py")
#    f.write("  \n")
#    f.close()





cursor.close()
cnx.close()
ser.close()



