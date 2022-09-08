from ophyd import Device, EpicsMotor, EpicsSignal, EpicsSignalRO
from ophyd import Component as Cpt
#import time
#import pandas as pd
#import numpy as np


LED = EpicsSignal('XF:28IDC-ES:1{Light:Flu-LED:1}Cmd', name='LED_M365LP1', string=True, kind='hinted')
# 0: 'Low'; 1: 'High'

deuterium = EpicsSignal('XF:28IDC-ES:1{Light:Abs-Dut:1}Cmd', name='Deuterium', string=True, kind='Config')
# 0: 'Low'; 1: 'High'

halogen = EpicsSignal('XF:28IDC-ES:1{Light:Abs-Hal:1}Cmd', name='Halogen', string=True, kind='Config')
# 0: 'Low'; 1: 'High'

UV_shutter = EpicsSignal('XF:28IDC-ES:1{Light:Abs-Sht:1}Cmd', name='UV_shutter', string=True, kind='hinted')
# 0: 'Low' --> shutter close.; 1: 'High' --> Shutter open.


def LED_on():
    LED.put('High', wait=True)
    return LED.get()
    
def LED_off():
    LED.put('Low', wait=True)
    return LED.get()
    
def shutter_open():
    UV_shutter.put('High', wait=True)
    return UV_shutter.get()
    
def shutter_close():
    UV_shutter.put('Low', wait=True)
    return UV_shutter.get()