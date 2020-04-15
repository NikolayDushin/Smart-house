#!/usr/bin/python3
# -*- coding: UTF-8 -*-

# Отработка изменений weather

import mysql.connector, sys
import datetime as dt

join = []
save = []
t_j = []


def add_to_save(save_counter, t2):
    #print(save_counter, t2)
    join = [save_counter, t2]
    save.append(join)
    save_counter = save_counter + 1
    #print(save[0], save_counter)
    return save, save_counter


def temperature_events(save_counter, save):
    #print(T_dif)    
    
    if T_dif > 10:
        t1_info = "В течении дня ожидается рост температуры на "
        t1 = t1_info + str(T_dif) + " C"
        save, save_counter = add_to_save(save_counter, t2)
        #join = [str(save_counter), t1]
        #save.append(join)
        #save_counter=save_counter+1
        return save, save_counter
         
    elif T_dif < -10:                           # Если температура упадет на 10 С за сутки
        t2_info = "В течении дня ожидается падение температуры на "
        t2 = t2_info + str(T_dif) + " C"
        save, save_counter = add_to_save(save_counter, t2)   
        #join = [save_counter, t2]
        #save.append(join)
        #save_counter = save_counter + 1
        print(save, save_counter)
        return save, save_counter


##########################################################
def test_add(x,y):
    join = [x, y]
    #print(join)
    t_j.append(join)
    #print(t_j)
    return t_j


def test_list(temp1, temp2):
    

    t_j = test_add('Температура = ', str(temp2))
    #t_j = test_add('Температура = ',temp2)
    #t_j = test_add('Температура = ',temp1)
    return t_j   


temp1 = 10
temp2 = 20

t_j = test_list(temp1, temp2)
print(t_j[0])

#print(t_j[0], t_j[1])
###############################################################

save_counter = 1

T_min = -5
T_max = -25
T_dif = float(T_max) - float(T_min)

#print(T_dif)

#save, save_counter = temperature_events(save_counter, save)

#print(save, save_counter)
#print(save_counter)









