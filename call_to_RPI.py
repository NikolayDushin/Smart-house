#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import time

import RPi.GPIO as GPIO
GPIO.setmode(GPIO.BCM)

GPIO.setup(23, GPIO.IN, pull_up_down=GPIO.PUD_UP)

#time.sleep(2)   # sec


n=0
while n<3:
   try:
      GPIO.wait_for_edge(23, GPIO.FALLING)
      print ("Alarm! Coming signal from Mega32!")
      os.system('python3 /home/pi/sh/python/main.py')
      #n=n+1

   except KeyboardInterrupt:      
      GPIO.cleanup() 