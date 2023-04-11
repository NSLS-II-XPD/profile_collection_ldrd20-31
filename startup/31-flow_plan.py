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


def insitu_test(abs_repeat, cor_repeat, csv_path=None, sample='rhodamine', pump_list=None, precursor_list=None, mixer=None, note=None, data_agent='db'):
    # yield from bps.sleep(2)
    for i in range(abs_repeat):
        yield from qepro.take_uvvis_save_csv(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Absorbtion', correction_type='Reference', 
                                              pump_list=pump_list, precursor_list=precursor_list, mixer=mixer, note=note, data_agent=data_agent)
    # yield from bps.sleep(2)    
    for j in range(cor_repeat):
        yield from qepro.take_uvvis_save_csv(sample_type=sample, csv_path=csv_path, 
                                              spectrum_type='Corrected Sample', correction_type='Dark', 
                                              pump_list=pump_list, precursor_list=precursor_list, mixer=mixer, note=note, data_agent=data_agent)


# def insitu_test2(abs_repeat, cor_repeat, csv_path=None, sample='rhodamine', pump_list=None, precursor_list=None):
#     for i in range(abs_repeat):
#         yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
#                                               spectrum_type='Absorbtion', correction_type='Reference', 
#                                               pump_list=pump_list, precursor_list=precursor_list)
        
#     for j in range(cor_repeat):
#         yield from qepro.take_uvvis_save_csv2(sample_type=sample, csv_path=csv_path, 
#                                               spectrum_type='Corrected Sample', correction_type='Dark', 
#                                               pump_list=pump_list, precursor_list=precursor_list)


def setup_collection_q(integration_time=100, num_spectra_to_average=10, buffer=3,
                       spectrum_type='Absorbtion', correction_type='Reference', 
                       electric_dark_correction=True):

    # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
    # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'

    yield from bps.abs_set(qepro.integration_time, integration_time, wait=True)
    yield from bps.abs_set(qepro.num_spectra, num_spectra_to_average, wait=True)
    yield from bps.abs_set(qepro.buff_capacity, buffer, wait=True)
    if num_spectra_to_average > 1:
        yield from bps.abs_set(qepro.collect_mode, 'Average', wait=True)
    else:
        yield from bps.abs_set(qepro.collect_mode, 'Single', wait=True)

    if electric_dark_correction:
        yield from bps.abs_set(qepro.electric_dark_correction, 1, wait=True)

    yield from bps.abs_set(qepro.correction, correction_type, wait=True)
    yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
    


def take_ref_bkg_q(integration_time=15, num_spectra_to_average=16, 
                   buffer=3, electric_dark_correction=True, ref_name='test', 
                   data_agent='db', csv_path=None):
    
    yield from qepro.setup_collection2(integration_time=integration_time, 
                                       num_spectra_to_average=num_spectra_to_average, buffer=buffer, 
                                       spectrum_type='Absorbtion', correction_type='Reference', 
                                       electric_dark_correction=True)
    
    # yield from LED_off()
    # yield from shutter_close()
    yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
    yield from bps.sleep(5)
    yield from qepro.get_dark_frame2()
    uid = (yield from count([qepro], md = {'note':'Dark'}))
    if csv_path != None:
        print(f'Export dark file to {csv_path}...')
        qepro.export_from_scan(uid, csv_path, sample_type=f'Dark_{integration_time}ms', data_agent=data_agent)

    yield from bps.mv(UV_shutter, 'High')
    yield from bps.sleep(2)
    yield from qepro.get_reference_frame2()
    uid = (yield from count([qepro], md = {'note':ref_name}))

    yield from bps.mv(LED, 'Low', UV_shutter, 'Low')

    if csv_path != None:
        print(f'Export reference file to {csv_path}...')
        qepro.export_from_scan(uid, csv_path, sample_type=f'{ref_name}_{integration_time}ms', data_agent=data_agent)





def take_uvvis_save_csv_q(sample_type='test', plot=False, csv_path=None, data_agent='tiled', 
                        spectrum_type='Absorbtion', correction_type='Reference', 
                        pump_list=None, precursor_list=None, mixer=None, note=None, md=None):
    
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list], 
                "precursors" : precursor_list, 
                "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list], 
                "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
                "pump_status" : [pump.status.get() for pump in pump_list], 
                "uvvis" :[spectrum_type, correction_type, qepro.integration_time.get(), qepro.num_spectra.get(), qepro.buff_capacity.get()], 
                "mixer": mixer,
                "sample_type": sample_type,
                "note" : note if note else "None"}
        _md.update(md or {})
    
    if (pump_list == None and precursor_list == None):
        _md = { "uvvis" :[spectrum_type, correction_type, qepro.integration_time.get(), qepro.num_spectra.get(), qepro.buff_capacity.get()], 
                "mixer": ['exsitu measurement'],
                "sample_type": sample_type,
                "note" : note if note else "None"}
        _md.update(md or {})
    
    # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
    # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'
    
    # qepro.correction.put(correction_type)
    # qepro.spectrum_type.put(spectrum_type)
    
    if spectrum_type == 'Absorbtion':
        if LED.get()=='Low' and UV_shutter.get()=='High' and qepro.correction.get()==correction_type and qepro.spectrum_type.get()==spectrum_type:
            uid = (yield from count([qepro], md=_md))
        else:
            yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            # yield from LED_off()
            # yield from shutter_open()
            yield from bps.mv(LED, 'Low', UV_shutter, 'High')
            yield from bps.sleep(2)
            uid = (yield from count([qepro], md=_md))
        
        
    else:
        if LED.get()=='High' and UV_shutter.get()=='Low' and qepro.correction.get()==correction_type and qepro.spectrum_type.get()==spectrum_type:
            uid = (yield from count([qepro], md=_md))
        else:
            yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            # yield from shutter_close()
            # yield from LED_on()
            yield from bps.mv(LED, 'High', UV_shutter, 'Low')
            yield from bps.sleep(2)
            uid = (yield from count([qepro], md=_md))
    
    yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
    
    if csv_path!=None or plot==True:
        yield from bps.sleep(2)
        qepro.export_from_scan(uid, csv_path, sample_type, plot=plot, data_agent=data_agent)

