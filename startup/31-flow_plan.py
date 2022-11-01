from ophyd import Device, EpicsMotor, EpicsSignal, EpicsSignalRO
from ophyd import Component as Cpt
import time
import pandas as pd
import numpy as np
from ophyd.sim import det4, noisy_det, motor  # simulated detector, motor
import h5py
from datetime import datetime

import bluesky.plan_stubs as bps
from bluesky.plans import count

import matplotlib.pyplot as plt
import glob

from bluesky import RunEngine
import bluesky.plan_stubs as bps
from bluesky.plans import count, scan
RE = RunEngine({})

## Prepare Live Visualization
from bluesky.callbacks.best_effort import BestEffortCallback
bec = BestEffortCallback()

# Send all metadata/data captured to the BestEffortCallback.
RE.subscribe(bec)

# Make plots update live while scans run.
# from bluesky.utils import install_kicker
# install_kicker()
#%matplotlib notebook
#from bluesky.utils import install_nb_kicker
#install_nb_kicker()


## Prepare Data Storage
from databroker import Broker
db = Broker.named('xpd')
                  
# from databroker import catalog
# db = catalog['xpd']
    
# Insert all metadata/data captured into db.
RE.subscribe(db.insert)


## Add a Progress Bar
from bluesky.utils import ProgressBarManager
RE.waiting_hook = ProgressBarManager()



def reset_pumps(pump_list, clear=True, update = '.2 second'):
    for pump in pump_list:
        pump.initialize_pump(clear=clear, update = update)
        pump.infuse_rate_unit.put('ul/min', wait=True)
        pump.infuse_rate.put(100, wait=True)
        pump.withdraw_rate_unit.put('ul/min', wait=True)
        pump.withdraw_rate.put(100, wait=True)
        pump.target_vol_unit.put('ml', wait=True)
        pump.target_vol.put(20, wait=True)


def show_pump_status(syringe_list, pump_list, precursor_list, wait=False):
    for input_size, pump, precursor in zip(syringe_list, pump_list, precursor_list):
        print('Name: ' + f'{pump.name}')
        print('Precursor: ' + f'{precursor}')
        pump.check_pump_condition(input_size, wait=wait)
        print('\n')
        

def set_group_infuse(syringe_list, pump_list, target_vol_list=['50 ml', '50 ml'], rate_list = ['100 ul/min', '100 ul/min']):
    for i, j, k, l in zip(pump_list, target_vol_list, infuse_rate_list, syringe_list):
        vol = float(j.split(' ')[0])
        vol_unit = j.split(' ')[1]
        rate = float(k.split(' ')[0])
        rate_unit = k.split(' ')[1]        
        yield from i.set_infuse(l, target_vol = vol, target_unit = vol_unit, infuse_rate = rate, infuse_unit = rate_unit)
        
def set_group_withdraw(syringe_list, pump_list, target_vol_list=['50 ml', '50 ml'], rate_list = ['100 ul/min', '100 ul/min']):
    for i, j, k, l in zip(pump_list, target_vol_list, rate_list, syringe_list):
        vol = float(j.split(' ')[0])
        vol_unit = j.split(' ')[1]
        rate = float(k.split(' ')[0])
        rate_unit = k.split(' ')[1]        
        yield from i.set_withdraw(l, target_vol = vol, target_unit = vol_unit, withdraw_rate = rate, withdraw_unit = rate_unit)


def start_group_infuse(pump_list):
    for pump in pump_list:
        yield from pump.infuse_pump2()
        

def start_group_withdraw(pump_list):
    for pump in pump_list:
        yield from pump.withdraw_pump2()
        

def stop_group(pump_list):
    for pump in pump_list:
        yield from pump.stop_pump2()
        


def _readable_time(unix_time):
    from datetime import datetime
    dt = datetime.fromtimestamp(unix_time)
    print(f'{dt.year}{dt.month:02d}{dt.day:02d},{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')
    return (f'{dt.year}{dt.month:02d}{dt.day:02d}'), (f'{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')


def insitu_test(abs_repeat, cor_repeat, csv_path=None, sample='rhodamine', pump_list=None, precursor_list=None):
    for i in range(abs_repeat):
        yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Absorbtion', correction_type='Reference', 
                                              pump_list=pump_list, precursor_list=precursor_list)
        
    for j in range(cor_repeat):
        yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Corrected Sample', correction_type='Dark', 
                                              pump_list=pump_list, precursor_list=precursor_list)