'''
pump_list = [dds1_p1]
syringe_list = [2.5]
target_vol_list = ['0.5 ml']
infuse_rates = ['500 ul/min']
precursor_list = ['test']
mixer = [None]
syringe_mater_list=['glass_H1000']
'''

def reset_pumps(pump_list, clear=True, update = '.2 second'):
    for pump in pump_list:
        pump.initialize_pump(clear=clear, update = update)
        pump.infuse_rate_unit.put('ul/min', wait=True)
        pump.infuse_rate.put(100, wait=True)
        pump.withdraw_rate_unit.put('ul/min', wait=True)
        pump.withdraw_rate.put(100, wait=True)
        pump.target_vol_unit.put('ml', wait=True)
        pump.target_vol.put(20, wait=True)


def reset_pumps2(pump_list, clear=True, update = '.2 second'):
    for pump in pump_list:
        pump.initialize_pump(clear=clear, update = update)
        yield from bps.mv(pump.infuse_rate_unit, 'ul/min', 
                          pump.infuse_rate, 100, 
                          pump.withdraw_rate_unit, 'ul/min', 
                          pump.withdraw_rate, 100, 
                          pump.target_vol_unit, 'ml', 
                          pump.target_vol, 20)


def show_pump_status(syringe_list, pump_list, precursor_list, syringe_mater_list, wait=False):
    for input_size, pump, precursor, material in zip(syringe_list, pump_list, precursor_list, syringe_mater_list):
        print('Name: ' + f'{pump.name}')
        print('Precursor: ' + f'{precursor}')
        pump.check_pump_condition(input_size, material, wait=wait)
        print('\n')
        

def set_group_infuse(syringe_list, pump_list, target_vol_list=['50 ml', '50 ml'], 
                     rate_list = ['100 ul/min', '100 ul/min'], syringe_mater_list=['steel', 'steel']):
    for i, j, k, l, m in zip(pump_list, target_vol_list, rate_list, syringe_list, syringe_mater_list):
        vol = float(j.split(' ')[0])
        vol_unit = j.split(' ')[1]
        rate = float(k.split(' ')[0])
        rate_unit = k.split(' ')[1]        
        yield from i.set_infuse(l, target_vol = vol, target_unit = vol_unit, infuse_rate = rate, 
                                infuse_unit = rate_unit, syringe_material=m)
        
def set_group_withdraw(syringe_list, pump_list, target_vol_list=['50 ml', '50 ml'], 
                       rate_list = ['100 ul/min', '100 ul/min'], syringe_mater_list=['steel', 'steel']):
    for i, j, k, l, m in zip(pump_list, target_vol_list, rate_list, syringe_list, syringe_mater_list):
        vol = float(j.split(' ')[0])
        vol_unit = j.split(' ')[1]
        rate = float(k.split(' ')[0])
        rate_unit = k.split(' ')[1]        
        yield from i.set_withdraw(l, target_vol = vol, target_unit = vol_unit, withdraw_rate = rate, 
                                  withdraw_unit = rate_unit, syringe_material=m)


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


def insitu_test(abs_repeat, cor_repeat, csv_path=None, sample='rhodamine', pump_list=None, precursor_list=None, mixer=None, note=None):
    # yield from bps.sleep(2)
    for i in range(abs_repeat):
        yield from qepro.take_uvvis_save_csv(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Absorbtion', correction_type='Reference', 
                                              pump_list=pump_list, precursor_list=precursor_list, mixer=mixer, note=note)
    # yield from bps.sleep(2)    
    for j in range(cor_repeat):
        yield from qepro.take_uvvis_save_csv(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Corrected Sample', correction_type='Dark', 
                                              pump_list=pump_list, precursor_list=precursor_list, mixer=mixer, note=note)


# def insitu_test2(abs_repeat, cor_repeat, csv_path=None, sample='rhodamine', pump_list=None, precursor_list=None):
#     for i in range(abs_repeat):
#         yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
#                                               spectrum_type='Absorbtion', correction_type='Reference', 
#                                               pump_list=pump_list, precursor_list=precursor_list)
        
#     for j in range(cor_repeat):
#         yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
#                                               spectrum_type='Corrected Sample', correction_type='Dark', 
#                                               pump_list=pump_list, precursor_list=precursor_list)