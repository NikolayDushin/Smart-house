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

def output_data(save_counter, t1):               #   Подготовка списка для занесения событий в таблицу
    join = [save_counter, t1]
    save.append(join)
    save_counter = save_counter + 1
    return save_counter

def output_data_e(save_counter_e, t1):               #   Подготовка списка для занесения экстремальных событий в таблицу
    join = [save_counter_e, t1]
    save_e.append(join)
    save_counter_e = save_counter_e + 1
    #print(save_counter_e, save_e)
    return save_counter_e




def temperature_events(save_counter, save_counter_e):

    #print(save_counter_e)

    #T_max = 70        

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
        save_counter_e = output_data_e(save_counter_e, t3)
        #print(save_e, save_counter_e)

    if T_min < -20:                        #   Если температура за сутки превысит 30С 
        t4_info = "В течении суток ожидается ожидается низкая температура "
        t4 = t4_info + str(T_min) + " C"
        #print(t4)
        save_counter_e = output_data_e(save_counter_e, t4)
        #print(save, save_counter)

    return save_counter, save_counter_e


def downfall_calculations():                     #   Подпрограмма расчетов, связанных с осадками
    i = 0
    D_sum = 0
    D_strong = ''
    D_very_strong = ''
    D_start = ''

    for y in data_o:
        y = float(y)
        D_sum = D_sum + y                            #   Вычисление общего количества осадков за сутки
        #print(round(D_sum,2)) 
        if y > 10 and D_very_strong == '':    
            D_very_strong = data_d[i]                #   Поиск времени начала очень сильных осадков
        if y > 5 and y < 10 and D_strong == '':    
            D_strong = data_d[i]                     #   Поиск времени начала сильных осадков
        if y > 0 and D_start == '':
            D_start = data_d[i] 
        i = i + 1
    return D_start, D_sum, D_strong, D_very_strong     


def downfall_events(save_counter, save_counter_e, D_start, D_sum, D_strong, D_very_strong):

    if D_start != '':                                                                              #   Если в течении суток ожидаются осадки                   
        D1 = ('Ожидаются осадки в '+ str(D_start)[8:10] + '-го в ' + str(D_start)[11:16] + '. ')
        D1 = D1 + (' За сутки выпадет ' + str(D_sum) + ' мм.')
        #print(save_counter,D1) 
        save_counter = output_data(save_counter, D1)

    if D_strong != '':                                                                             #   Если выпадет более 5 мм осадков
        D2 = ('Ожидаются сильные осадки в '+ str(D_strong)[8:10] + '-го в ' + str(D_strong)[11:16] + '. ')
        D2 = D2 + (' За сутки выпадет ' + str(D_sum) + ' мм.')
        #print(save_counter,D2) 
        save_counter = output_data(save_counter, D2)

    if D_very_strong != '':                                                                        #   Если выпадет более 5 мм осадков
        D3 = ('Ожидаются очень сильные осадки в '+ str(D_very_strong)[8:10] + '-го в ' + str(D_very_strong)[11:16] + '. ')
        D3 = D3 + (' За сутки выпадет ' + str(D_sum) + ' мм.')
        save_counter_e = output_data_e(save_counter_e, D3)
        #print(save_counter_e, D3)

    if D_sum > 55:                                                                        #   Если выпадет более 10 мм осадков
        D3 = ('Ожидаются превышение среднесуточной нормы. ')
        D3 = D3 + (' За сутки выпадет ' + str(D_sum) + ' мм.')
        #print(save_counter,D3) 
        save_counter_e = output_data_e(save_counter_e, D3)

    return save_counter, save_counter_e

def pressure_events(save_counter, P_min, P_max, P_dif):

    if P_min < 740:                               # Определение низкого давления, 740 мм
        p1_info = "В течении дня ожидается низкое давление "
        p1 = p1_info + str(P_min) + " мм. рт. ст."
        save_counter = output_data(save_counter, p1)
        #print(save_counter, p1)

    if P_max > 760:               # Определение высокого давления, более 760 мм
        p2_info = "В течении дня ожидается высокое давление "
        p2 = p2_info + str(P_max) + " мм. рт. ст."
        save_counter = output_data(save_counter, p2)
        #print(save_counter, p2)

    if P_dif > 5:               # Определение изменения давления за сутки более 5мм 
        p3_info = "Ожидается изменение давления более "
        p3 = p3_info + str(P_dif) + " мм. рт. ст."
        save_counter = output_data(save_counter, p3)
        #print(save_counter, p3)

    return save_counter, save_counter_e


def wind_calculations():
    i = 0
    D_wind_strong = ''
    D_wind_very_strong = ''
    D_storm = ''
 
    for z in data_w:
        z = float(z)
        if z > 5 and D_wind_strong == '':    
            D_wind_strong = data_d[i]            #   Поиск времени начала сильного ветра
        if z > 10 and D_wind_very_strong == '':    
            D_wind_very_strong = data_d[i]       #   Поиск времени начала очень сильного ветра
        if z > 15 and D_storm == '':
            D_storm = data_d[i]       #   Поиск времени начала очень сильного ветра
        i = i + 1

    if D_wind_very_strong != '': D_wind_strong = '' 
    if D_storm != '': 
        D_wind_strong = ''
        D_wind_very_strong = ''
  
    return D_wind_strong, D_wind_very_strong, D_storm 




def wind_events(save_counter, save_counter_e, D_wind_strong, D_wind_very_strong, D_storm):

    if D_wind_strong != '':
        t1_info = "Ожидается сильный ветер "
        t1 = t1_info + str(D_wind_strong)[8:10] + '-го в ' + str(D_wind_strong)[11:16] 
        #print (t1)
        save_counter = output_data(save_counter, t1)

    if D_wind_very_strong != '':
        t2_info = "Ожидается очень сильный ветер "
        t2 = t2_info + str(D_wind_very_strong)[8:10] + '-го в ' + str(D_wind_very_strong)[11:16] 
        #print (t2)
        save_counter = output_data(save_counter, t2)

    if D_storm != '':
        t3_info = "Ожидается штормовой ветер "
        t3 = t3_info + str(D_storm)[8:10] + '-го в ' + str(D_storm)[11:16] 
        #print (t3)
        save_counter_e = output_data_e(save_counter_e, t3)
    return save_counter, save_counter_e


def filling_weather_alarm_chart():
    statmt = "DELETE FROM `weather_alarm` WHERE w_a_id > 0"                     # Удаление всей информации из таблицы weather_alarm
    cursor.execute(statmt)
    cnx.commit()

    if len(save) > 0:                                                               # Если есть информация для записи, то записываем 
        stmt = "INSERT INTO weather_alarm (w_a_id, a_info) VALUES (%s, %s)"
    else:                                                                           #   Если нет информации, то отпрвляем строку
        t1_info = "Опасных природных явлений в ближайшие сутки не ожидается"
        join = [1, t1_info]
        save.append(join)
        stmt = "INSERT INTO weather_alarm (w_a_id, a_info) VALUES (%s, %s)"
    cursor.executemany(stmt, save)
    cnx.commit()



def filling_weather_real():
    cursor.execute("SELECT * from weather_real")
    results = cursor.fetchall()   # Распечатка столбца, только данные, без скобок
    last_line = cursor.rowcount

    r_d = 0
    for row in results:                          # Заносим данные из weather_real в массивы 
        r_d = row[1]
        data_rd.append(r_d)
        r_t=row[2]
        data_rt.append(r_t)
        r_p=row[3]
        data_rp.append(r_p)

    
    w_r_id = last_line + 1                       # Новые данные
    w_d = data_d[0]
    w_t = data_t[0]
    w_p = data_p[0]


    if last_line < 25:                           # Если количество элементов в таблице менее 25, то добавляем элементы
        if (r_d != w_d):                         #   Проверка на задвоение, если запуск подпрограммы происходит более 1 раза в час
            cursor.execute("""INSERT INTO weather_real VALUES (%s,%s,%s,%s)""",(w_r_id, w_d, w_t, w_p)) 
            cnx.commit()
    else:                                        #  Если таблица уже заполнена, то переносим данные в массивы
        data_rd.append(w_d)                      # Заносим новые данные в массивы
        data_rt.append(w_t)
        data_rp.append(w_p)

                                                 #   Строка №1 уходит, ее заменяет строка из новых данных
        i = 1
        if (r_d != w_d):                         #   Проверка на задвоение, если запуск подпрограммы происходит более 1 раза в час
            while i < 26:                        # Значение 26
                sts = """UPDATE `weather_real` SET w_d=%s, w_t=%s, w_p=%s  WHERE w_r_id=%s"""
                datas=(data_rd[i], data_rt[i], data_rp[i], i)    
                cursor.execute(sts,datas)
                cnx.commit()
                #print (data_rd[i], data_rt[i], data_rp[i], i)
                i = i + 1



def pressure_analysis(save_counter_e):
    delta_t = float(data_rt[23])-float(data_rt[18])
    delta_t = round(delta_t,2)
    if delta_t > 4:
        t1_info = "Внимание! Резкое повышение температуры за последние 5 часов "
        t1 = t1_info + 'на ' + str(delta_t) + ' градусов'
        save_counter_e = output_data_e(save_counter_e, t1)
    if delta_t < -4:
        t1_info = "Внимание! Резкое понижение температуры за последние 5 часов "
        t1 = t1_info + 'на ' + str(delta_t) + ' градусов'
        save_counter_e = output_data_e(save_counter_e, t1)






   

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



######################  Расчеты
    
T_min = float(min(data_t))
T_max = float(max(data_t))
T_dif = float(T_max) - float(T_min)



D_start, D_sum, D_strong, D_very_strong = downfall_calculations()
D_sum = round(D_sum,1)                                                                             #   Округление 


P_min = float(min(data_p))   # минимальное давление за сутки
P_max = float(max(data_p))   # максимальное давление за сутки
P_dif = P_max - P_min  # разница

D_wind_strong, D_wind_very_strong, D_storm = wind_calculations()





###################################   Анализ данных  ####################################################

save_counter, save_counter_e = temperature_events(save_counter, save_counter_e)  #   Проверка событий, связанных с температурой 

save_counter, save_counter_e = downfall_events(save_counter, save_counter_e, D_start, D_sum, D_strong, D_very_strong)

save_counter, save_counter_e = pressure_events(save_counter, P_min, P_max, P_dif)

save_counter, save_counter_e = wind_events(save_counter, save_counter_e, D_wind_strong, D_wind_very_strong, D_storm)

################################### Занесение информации в таблицу weather_alarm  #####################################

filling_weather_alarm_chart()

################################### Занесение информации в таблицу weather_real  #####################################

filling_weather_real()

##################################   Анализ данных в таблице weather_real   ##############################

pressure_analysis(save_counter_e)





### Проверка резкого изменения давления за последние 5 часов


res_delta_p = 0    # Переменная, указывает на время роста температуры в течении 5 часов на 5 градусов
res_delta_pm = 0   # Переменная, указывает на время падения температуры в течении 5 часов на 5 градусов

delta_p = int(data_rp[23])-int(data_rp[18])

#delta_p=-5

if delta_p > 4:
    t1_info = "Внимание! Резкое повышение атмосферного давления за последние 5 часов "
    t1 = t1_info
    #join = [save_counter_e, t1]
    #save_e.append(join)
    #save_counter_e = save_counter_e +1
    #print data_rp[23] 

if delta_p < -4:
    t1_info = "Внимание! Резкое понижение атмосферного давления за последние 5 часов "
    t1 = t1_info
    #join = [save_counter_e, t1]
    #save_e.append(join)
    #save_counter_e = save_counter_e +1
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
  