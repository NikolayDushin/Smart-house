#!/usr/bin/python3
# -*- coding: UTF-8 -*-

#######   Подпрограмма запускается из CRON и анализирует входящие погодные данные. Выходная информация отображается на сайте.  

# Вопрос по росту Т-ры на 10,3С и повышению давления.
## Этот скрипт:
##   1. Считывает данные из таблицы forecast
##   2. Анализирует данные
##   3. Заносит информацию в таблицу weather_alarm
##   4. Считывает данные из таблицы weather_real 
##   5. Если время ок, то записывает строку в конец таблицы weather_real 
##   6. Удаляется первая строка

import mysql.connector, sys
import datetime as dt

data_d = []
data_t = []
data_p = []
data_o = []
data_w = []
save = []                                        #   Список для публикации
save_test = []
save_e = []
save_2 = []
alarm_data = []
note = []
join = []
data_id = []
data_rd = []
data_rt = []
data_rp = []

##############################################   Подпрограммы   #######################################################

def keys():                                # Cчитать пароль к базе данных
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
    full_log_line = str(current_time) + ' - ' + 'weather.py' + ' - ' + error_name + ' - ' + error_description 
    f = open("/home/pi/sh/python/logs/errors.log", "a")   
    f.write(full_log_line)
    f.write("  \n") 
    f.close()
 
def receiving_data():
    for row in results:
        y_d = row[1]
        data_d.append(y_d)            # Занесение данных дата/время в массив 
        y_t = row[2]
        data_t.append(y_t)            # Занесение данных температуры в массив 
        y_p = row[3]
        data_p.append(y_p)            # Занесение данных давления в массив         
        y_o = row[4]
        data_o.append(y_o)            # Занесение данных по осадкам в массив 
        y_w = row[5]
        data_w.append(y_w)            # Занесение данных по силе ветра в массив 
    return y_d, y_t, y_p, y_o, y_w

def output_data(save_counter, t1):
    join = [save_counter, t1]
    save.append(join)
    save_counter = save_counter + 1
    return save_counter

def temperature_events(save_counter):

    #T_dif = -30

    if T_dif > 10:                            # Если температура вырастет более чем на 10 С за сутки 
        t1_info = "В течении суток ожидается рост температуры на "
        t1 = t1_info + str(T_dif) + " C"
        save_counter = output_data(save_counter, t1)
  
    if T_dif < -10:                           # Если температура упадет более чем на 10 С за сутки
        t2_info = "В течении суток ожидается падение температуры на "
        t2 = t2_info + str(T_dif) + " C" 
        save_counter = output_data(save_counter, t2)
 
    if T_max > 29:                        #   Если температура за сутки превысит 30С 
        t3_info = "В течении суток ожидается высокая температура "
        t3 = t3_info + str(T_max) + " C"
        #print(t3)
        save_counter = output_data(save_counter, t3)
        #print(save, save_counter)

    if T_min < -20:                        #   Если температура за сутки превысит 30С 
        t4_info = "В течении суток ожидается ожидается низкая температура "
        t4 = t4_info + str(T_min) + " C"
        #print(t4)
        save_counter = output_data(save_counter, t4)
        #print(save, save_counter)

    return save_counter
    



def downfall():

    i = 0
    flag_min_osadki = 0
    flag_max_osadki = 0
    #flag_extrim_osadki = 0



    for osadki_mm in data_o:                     # Поиск количества осадков более 0мм и определение времени  
        if float(osadki_mm) > 10:                 # Убрать из списка нулевые значения
            #print(i, data_d[i], )
            return i 
#        else: break
        i=i+1
       







def downfall_events(save_counter):
    i = downfall()
    print(i)
    d1 = ('Ожидаются осадки '+ str(data_d[i])[8:10] + '-го в ' + str(data_d[i])[11:16] + '. ')
    d1 = d1 + ('Количество ' + str(data_o[i]) + ' мм.')
    print(d1, save_counter)
    #save_counter = output_data(save_counter, d1)




    #return save_counter









    

   

##############################################   Подключение к базе MySQL   ###########################################

user_value, db_value, pw_value = keys()    #   Cчитать пароль к базе данных    

######################## Получение данных

cnx = mysql.connector.connect(user = user_value, database = db_value, password = pw_value, charset='utf8')
cursor = cnx.cursor()
cursor.execute("SELECT * from forecast")         #   Извлечние входящих данных из таблицы 
results = cursor.fetchall()   

y_d, y_t, y_p, y_o, y_w =  receiving_data()      #   Разбивка данных 

save_counter_e = 1
save_counter = 1                                 #   Счетчик сообщений, которые будут опубликованы 


    
T_min = float(min(data_t))
T_max = float(max(data_t))
T_dif = float(T_max) - float(T_min)

###################################   Анализ данных  ####################################################

save_counter = temperature_events(save_counter)  #   Проверка событий, связанных с температурой 





downfall_events(save_counter)


#save_counter = downfall_events(save_counter)










########## События, связанные с осадками ##########

i = 0
flag_min_osadki = 0
flag_max_osadki = 0
flag_extrim_osadki = 0

for osadki_mm in data_o:      # Поиск количества осадков более 1мм и определение времени  
    if float(osadki_mm) > 0:  # Поиск времени начала дождя 
        if flag_min_osadki == 0: 
            time_o_min = str(data_d[i])[8:16]
            time_event = time_o_min[0:8]
            time_refresh = str(data_d[0])[8:16]
            #print time_event
            #print time_refresh
            if time_refresh < time_event:            # Если время события прошло 
                flag_min_osadki = 1
                t1_info = "Ожидаются осадки "
                t1 = t1_info + str(time_o_min)
                join = [save_counter, t1]
                save.append(join)
                save_counter = save_counter +1 

    if float(osadki_mm) > 5:  # Поиск времени начала сильного дождя 
        if flag_max_osadki == 0: 
            time_o_max = str(data_d[i])[8:16]
            flag_max_osadki = 1
            t1_info = "Ожидаются сильные осадки "
            t1 = t1_info + str(time_o_max)
            join = [save_counter, t1]
            save.append(join)
            save_counter = save_counter +1 

    if float(osadki_mm) > 10:  # Поиск времени начала очень сильного дождя 
        if flag_extrim_osadki == 0: 
            time_o_extreme = str(data_d[i])[8:16]
            flag_extrim_osadki = 1
            t1_info = "Внимание! Опасный уровень осадков "
            t1 = t1_info + str(time_o_extreme)
            join = [save_counter_e, t1]
            save_e.append(join)
            save_counter_e = save_counter_e +1

    i=i+1

########## События, связанные с атмосферным давлением   ##########

p_min = float(min(data_p))   # минимальное давление за сутки
p_max = float(max(data_p))   # максимальное давление за сутки

p_dif = p_max - p_min  # разница

if p_min < 740:               # Определение низкого давления 
    p1_info = "В течении дня ожидается низкое давление "
    p1 = p1_info + str(p_min) + " мм рт ст"
    join = [save_counter, p1]
    save.append(join)
    save_counter = save_counter +1 
    #print p_min

if p_max > 760:               # Определение высокого давления 
    p2_info = "В течении дня ожидается высокое давление "
    p2 = p2_info + str(p_max) + " мм рт ст"
    join = [save_counter, p2]
    save.append(join)
    save_counter = save_counter +1
    #print p_max

if p_dif > 5:               # Определение изменения давления за сутки более 5мм 
    p2_info = "Ожидается изменение давления более "
    p2 = p2_info + str(p_dif) + " мм рт ст"
    join = [save_counter, p2]
    save.append(join)
    save_counter = save_counter +1

#print p_min
#print p_max
#print p_dif

########## События, связанные с ветром   ##########

i = 0
#w_max = float(max(data_w))   # максимальная сила ветра за сутки

flag_max_wind = 0

for wind in data_w:      # Поиск силы ветра более 5 м/с и определение времени
    if float(wind) > 5:  # Поиск времени начала сильного ветра 
        if flag_max_wind == 0: 
            time_w = str(data_d[i])[8:16]
            time_event = time_w[0:8]
            time_refresh = str(data_d[0])[8:16]
            if time_refresh < time_event:                   # Если время события прошло
                flag_max_wind = 1
                t1_info = "Ожидается сильный ветер "
                t1 = t1_info + str(time_w)
                join = [save_counter, t1]
                save.append(join)
                save_counter = save_counter +1
    i=i+1



i = 0
flag_max_wind = 0

for wind in data_w:      # Поиск силы ветра более 10м/c и определение времени  
    if float(wind) > 10:  # Поиск времени начала сильного ветра 
        if flag_max_wind == 0: 
            time_w = str(data_d[i])[8:16]
            print (time_w)
            flag_max_wind = 1
            t1_info = "Ожидается очень сильный ветер "
            t1 = t1_info + str(time_w)
            join = [save_counter, t1]
            save.append(join)
            save_counter = save_counter +1
    i=i+1

i = 0
flag_extreme_wind = 0

for wind in data_w:      # Поиск силы ветра более 15м/c и определение времени  
    if float(wind) > 15:  # Поиск времени начала сильного ветра 
        if flag_extreme_wind == 0: 
            time_w_extreme = str(data_d[i])[8:16]
            #print time_w
            flag_extreme_wind = 1
            t1_info = "Внимание! Опасный штормовой ветер "
            t1 = t1_info + str(time_w_extreme)
            join = [save_counter_e, t1]
            save_e.append(join)
            save_counter_e = save_counter_e +1
    i=i+1

################################### Занесение информации в таблицу weather_alarm  #####################################

# Удаление всей информации из таблицы weather_alarm
statmt = "DELETE FROM `weather_alarm` WHERE w_a_id > 0" 
cursor.execute(statmt)
cnx.commit()

#print len(save)

if len(save) > 0:          # Если есть информация для записи   
    stmt = "INSERT INTO weather_alarm (w_a_id, a_info) VALUES (%s, %s)"
    cursor.executemany(stmt, save)
    cnx.commit()
else:
    t1_info = "Опасных природных явлений в ближайшие сутки не ожидается"
    join = [1, t1_info]
    save.append(join)
    stmt = "INSERT INTO weather_alarm (w_a_id, a_info) VALUES (%s, %s)"
    cursor.executemany(stmt, save)
    cnx.commit()


################################### Занесение информации в таблицу weather_real  #####################################
########################################################################################################

cursor.execute("SELECT * from weather_real")
results = cursor.fetchall()   # Распечатка столбца, только данные, без скобок
last_line = cursor.rowcount

r_d = 0

### Заносим данные из weather_real в массивы 
for row in results:
    r_d = row[1]
    data_rd.append(r_d)
    r_t=row[2]
    data_rt.append(r_t)
    r_p=row[3]
    data_rp.append(r_p)
    #print r_d


### Новые данные
w_r_id = last_line + 1
w_d = data_d[0]
w_t = data_t[0]
w_p = data_p[0]
#print w_r_id, w_d, w_t, w_p

#print last_line

### Если количество элементов в таблице менее 25, то добавляем элементы

if last_line < 25:          # Значение 25
    if (r_d != w_d):
        cursor.execute("""INSERT INTO weather_real VALUES (%s,%s,%s,%s)""",(w_r_id, w_d, w_t, w_p)) 
        cnx.commit()

###  Если таблица уже заполнена, то переносим данные в массивы
else:
    ### Заносим новые данные в массивы
    data_rd.append(w_d)
    data_rt.append(w_t)
    data_rp.append(w_p)

    ### Строка №1 уходит, ее заменяет строка из новых данных
    i = 1
    while i < 26:           # Значение 26
        sts = """UPDATE `weather_real` SET w_d=%s, w_t=%s, w_p=%s  WHERE w_r_id=%s"""
        datas=(data_rd[i], data_rt[i], data_rp[i], i)    
        cursor.execute(sts,datas)
        cnx.commit()
        #print data_rd[i], data_rt[i], data_rp[i], i
        i = i + 1


#print w_d
#print r_d

#print data_d[0], data_t[0], data_p[0]
#print data_d[1], data_t[1], data_p[1]







#for row in results:
#    r_id=row[0]
#    data_id.append(r_id)            # Занесение данных ID в массив
#    r_d=row[1]
#    data_rd.append(r_d)
#    r_t=row[2]
#    data_rt.append(r_t)
#    r_p=row[3]
#    data_rp.append(r_p)


##### Занесение новой информации в таблицу

#w_r_id=max(data_id) + 1
#w_d = data_d[0]
#w_t = data_t[0]
#w_p = data_p[0]

#### Добавляем строку

#if r_d <> w_d:       # Добавление строки только если время увеличивается  
#    cursor.execute("""INSERT INTO weather_real VALUES (%s,%s,%s,%s)""",(w_r_id, w_d, w_t, w_p)) 
#    cnx.commit()

#### Удаляем старую строку

#if last_line > 24:
#    sts = "DELETE FROM `weather_real` ORDER BY w_r_id ASC LIMIT 1"
#    cursor.execute(sts)
#    cnx.commit()

#### Вариант UPDATE, не используется

#g=555
#title=11

#sts = """UPDATE `weather_real` SET w_p=%s WHERE w_r_id=%s"""
#datas=(g,title)
#cursor.execute(sts,datas)
#cnx.commit()

##################################   Считывание и анализ данных в таблице weather_real   ##############################

### Проверка резкого изменения температуры за последние 5 часов

res_delta_t = 0    # Переменная, указывает на время роста температуры в течении 5 часов на 5 градусов
res_delta_tm = 0   # Переменная, указывает на время падения температуры в течении 5 часов на 5 градусов

try:
   delta_t = float(data_rt[23])-float(data_rt[18])
except ValueError:
   #delta_t = int(abs(data_rt[23]))-int(abs(data_rt[18]))
   print("ValueError")
   sys.exit()


if delta_t > 4:
    t1_info = "Внимание! Резкое повышение температуры за последние 5 часов "
    t1 = t1_info
    join = [save_counter_e, t1]
    save_e.append(join)
    save_counter_e = save_counter_e +1
    #print data_rd[23]
  
if delta_t < -4:
    t1_info = "Внимание! Резкое понижение температуры за последние 5 часов "
    t1 = t1_info
    join = [save_counter_e, t1]
    save_e.append(join)
    save_counter_e = save_counter_e +1
    #print data_rd[23]



#print delta_t

#print data_rd[res_delta_t] 

### Проверка резкого изменения давления за последние 5 часов


res_delta_p = 0    # Переменная, указывает на время роста температуры в течении 5 часов на 5 градусов
res_delta_pm = 0   # Переменная, указывает на время падения температуры в течении 5 часов на 5 градусов

delta_p = int(data_rp[23])-int(data_rp[18])

#delta_p=-5

if delta_p > 4:
    t1_info = "Внимание! Резкое повышение атмосферного давления за последние 5 часов "
    t1 = t1_info
    join = [save_counter_e, t1]
    save_e.append(join)
    save_counter_e = save_counter_e +1
    #print data_rp[23] 

if delta_p < -4:
    t1_info = "Внимание! Резкое понижение атмосферного давления за последние 5 часов "
    t1 = t1_info
    join = [save_counter_e, t1]
    save_e.append(join)
    save_counter_e = save_counter_e +1
    #print data_rp[23]



################################### Занесение информации в таблицу weather_extreme  #####################################

# Удаление всей информации из таблицы weather_extreme
statmt = "DELETE FROM `weather_extreme` WHERE w_e_id > 0" 
cursor.execute(statmt)
cnx.commit()

if len(save_e) > 0:          # Если есть информация для записи   
    stmt = "INSERT INTO weather_extreme (w_e_id, e_info) VALUES (%s, %s)"
    cursor.executemany(stmt, save_e)
    cnx.commit()
else:
    t1 = "Экстримальных природных явлений в ближайшие сутки не ожидается"
    #w_e_id = 1
    join = [1, t1]
    save_e.append(join)
    stmt = "INSERT INTO weather_extreme (w_e_id, e_info) VALUES (%s, %s)"
    cursor.executemany(stmt, save_e)
    cnx.commit()



cursor.close()  
  