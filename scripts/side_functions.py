import datetime
import matplotlib.pyplot as plt
import time
import numpy as np
import pandas as pd
import os


def _readable_time(unix_time):
    from datetime import datetime
    dt = datetime.fromtimestamp(unix_time)
    print(f'{dt.year}{dt.month:02d}{dt.day:02d},{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')
    return (f'{dt.year}{dt.month:02d}{dt.day:02d}'), (f'{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')


def _data_keys():
    qepro_list=['QEPro_x_axis', 'QEPro_output', 'QEPro_sample', 'QEPro_dark', 'QEPro_reference', 
                'QEPro_spectrum_type', 'QEPro_integration_time', 'QEPro_num_spectra', 'QEPro_buff_capacity']
    qepro_dic = {}

    metadata_list=['uid', 'time', 'pumps','precursors','infuse_rate','infuse_rate_unit',
                   'pump_status', 'mixer','sample_type', 'note']
    metadata_dic = {}

    return qepro_list, qepro_dic, metadata_list, metadata_dic


### the export funs below are revised from self.export_from_scan in 10-QEPro.py
def export_qepro_by_stream(uid, csv_path, stream_name='primary', data_agent='catalog', plot=False, wait=False):
    if wait==True:
        time.sleep(2)
    
    qepro_dic, metadata_dic = read_qepro_by_stream(uid, stream_name=stream_name, data_agent=data_agent)
    dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name)
    print(f'Export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(csv_path)} done!')
    

def read_qepro_by_stream(uid, stream_name='primary', data_agent='catalog'):
    
    if data_agent == 'catalog':
        try:
            catalog
        except NameError:
            import databroker
            catalog = databroker.catalog['xpd-ldrd20-31']
    
    if data_agent == 'tiled':
        try:
            from_profile
        except NameError:
            from tiled.client import from_profile
            tiled_client = from_profile("xpd-ldrd20-31") 
    
    run = catalog[uid]
    meta = run.metadata
    
    try:
        data = run[stream_name].read()
        qepro_list, qepro_dic, metadata_list, metadata_dic = _data_keys()

        for i in qepro_list:
            qepro_dic[i] = data[i]

        for i in metadata_list:
            if i in meta['start'].keys():
                metadata_dic[i] = meta['start'][i]
            else:
                metadata_dic[i] = [None]

    except (KeyError, AttributeError):
        qepro_dic, metadata_dic = {}, {}
        print(f"Stream name: {stream_name} doesn't exist.")
    
    return qepro_dic, metadata_dic



def dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name='primary'):
        
    spectrum_type = qepro_dic['QEPro_spectrum_type'].values
    sample_type = metadata_dic['sample_type']
    date, time = _readable_time(metadata_dic['time'])
    full_uid = metadata_dic['uid']
    int_time = qepro_dic['QEPro_integration_time'].values
    num_average = qepro_dic['QEPro_num_spectra'].values
    boxcar_width = qepro_dic['QEPro_buff_capacity'].values
    pump_names = metadata_dic['pumps']
    precursor = metadata_dic['precursors']
    infuse_rate = metadata_dic['infuse_rate']
    infuse_rate_unit = metadata_dic['infuse_rate_unit']
    pump_status = metadata_dic['pump_status']
    mixer = metadata_dic['mixer']
    note = metadata_dic['note']

    x_axis_data = qepro_dic['QEPro_x_axis'].values
    dark_data = qepro_dic['QEPro_dark'].values
    reference_data = qepro_dic['QEPro_reference'].values
    sample_data = qepro_dic['QEPro_sample'].values
    output_data = qepro_dic['QEPro_output'].values

    if stream_name is 'primary':
        if spectrum_type == 3:
            spec = 'Abs'
            fout = f'{csv_path}/{sample_type}_{spec}_{date}-{time}_{uid[0:8]}.csv'
            
        elif spectrum_type == 2:
            spec = 'PL'
            fout = f'{csv_path}/{sample_type}_{spec}_{date}-{time}_{uid[0:8]}.csv'

        with open(fout, 'w') as fp:
            fp.write(f'uid,{full_uid}\n')
            fp.write(f'Time_QEPro,{date},{time}\n')
            fp.write(f'Integration time (ms),{int_time[0]}\n')
            fp.write(f'Number of averaged spectra,{num_average[0]}\n')
            fp.write(f'Boxcar width,{boxcar_width[0]}\n')

            for i in range(len(pump_names)):
                fp.write(f'{pump_names[i]},{precursor[i]},{infuse_rate[i]},{infuse_rate_unit[i]},{pump_status[i]}\n')
        
            if mixer != None:
                for i in range(len(mixer)):
                    fp.write(f'Mixer no. {i+1},{mixer[i]}\n')

            if type(note) is str:
                fp.write(f'Note,{note}\n')

            if spectrum_type == 3:
                fp.write('Wavelength,Dark,Reference,Sample,Absorbance\n')
            else:
                fp.write('Wavelength,Dark,Sample,Fluorescence\n')

            for i in range(x_axis_data.shape[1]):
                if spectrum_type == 3:
                    fp.write(f'{x_axis_data[0,i]},{dark_data[0,i]},{reference_data[0,i]},{sample_data[0,i]},{output_data[0,i]}\n')
                else:
                    fp.write(f'{x_axis_data[0,i]},{dark_data[0,i]},{sample_data[0,i]},{output_data[0,i]}\n')
    
    else:
        new_dir = f'{csv_path}/{date}{time}_{uid[0:8]}_{stream_name}'
        os.makedirs(new_dir, exist_ok=True)
        for j in range(x_axis_data.shape[0]):
            fout = f'{new_dir}/{sample_type}_{date}-{time}_{uid[0:8]}_{j:03d}.csv'
            
            with open(fout, 'w') as fp:
                fp.write(f'uid,{full_uid}\n')
                fp.write(f'Time_QEPro,{date},{time}\n')
                fp.write(f'Integration time (ms),{int_time[j]}\n')
                fp.write(f'Number of averaged spectra,{num_average[j]}\n')
                fp.write(f'Boxcar width,{boxcar_width[j]}\n')

                for i in range(len(pump_names)):
                    fp.write(f'{pump_names[i]},{precursor[i]},{infuse_rate[i]},{infuse_rate_unit[i]},{pump_status[i]}\n')
            
                if mixer != None:
                    for i in range(len(mixer)):
                        fp.write(f'Mixer no. {i+1},{mixer[i]}\n')

                if type(note) is str:
                    fp.write(f'Note,{note}\n')

                if spectrum_type[0] == 3:
                    fp.write('Wavelength,Dark,Reference,Sample,Absorbance\n')
                else:
                    fp.write('Wavelength,Dark,Sample,Fluorescence\n')

                for i in range(x_axis_data.shape[1]):
                    if spectrum_type[0] == 3:
                        fp.write(f'{x_axis_data[j,i]},{dark_data[j,i]},{reference_data[j,i]},{sample_data[j,i]},{output_data[j,i]}\n')
                    else:
                        fp.write(f'{x_axis_data[j,i]},{dark_data[j,i]},{sample_data[j,i]},{output_data[j,i]}\n')




def export_qepro_from_uid(uid, csv_path, sample_type=None, plot=False, data_agent='db', wait=False):
    if wait==True:
        time.sleep(2)
    
    if data_agent == 'db':
        qepro_dic, metadata_dic = read_qepro_from_db(uid)
    elif data_agent == 'tiled':
        qepro_dic, metadata_dic = read_qepro_from_tiled(uid)

    dic_to_csv(csv_path, qepro_dic, metadata_dic)
    print(f'Export uid: {uid[0:8]} to ../{os.path.basename(csv_path)} done!')



def read_qepro_from_tiled(uid):
    try:
       from_profile
    except NameError:
        from tiled.client import from_profile
        tiled_client = from_profile("xpd-ldrd20-31")  
    
    run = tiled_client[uid]
    ds = run.primary.read()
    meta = run.metadata
    
    # date, time = _readable_time(ds['time'][0])
    # full_uid = meta['start']['uid']
    
    qepro_list, qepro_dic, metadata_list, metadata_dic = _data_keys()

    for i in qepro_list:
        qepro_dic[i] = ds[i].values[0]

    for i in metadata_list:
        if i in meta['start'].keys():
            metadata_dic[i] = meta['start'][i]
        else:
            metadata_dic[i] = [None]
    
    return qepro_dic, metadata_dic



def read_qepro_from_db(uid):
    try:
        db
    except NameError:
        import databroker
        db = databroker.Broker.named('xpd-ldrd20-31')
    
    # full_uid = db[uid].start['uid']
    # unix_time = db[uid].start['time']     
    # date, time = _readable_time(unix_time)

    qepro_list, qepro_dic, metadata_list, metadata_dic = _data_keys()

    for i in qepro_list:
        qepro_dic[i] = db[uid].table()[i][1]

    for i in metadata_list:
        if i in db[uid].start.keys():
            metadata_dic[i] = db[uid].start[i]
        else:
            metadata_dic[i] = [None]
    
    return qepro_dic, metadata_dic




def dic_to_csv(csv_path, qepro_dic, metadata_dic):
        
    spectrum_type = qepro_dic['QEPro_spectrum_type']
    sample_type = metadata_dic['sample_type']
    date, time = _readable_time(metadata_dic['time'])
    full_uid = metadata_dic['uid']
    int_time = qepro_dic['QEPro_integration_time']
    num_average = qepro_dic['QEPro_num_spectra']
    boxcar_width = qepro_dic['QEPro_buff_capacity']
    pump_names = metadata_dic['pumps']
    precursor = metadata_dic['precursors']
    infuse_rate = metadata_dic['infuse_rate']
    infuse_rate_unit = metadata_dic['infuse_rate_unit']
    pump_status = metadata_dic['pump_status']
    mixer = metadata_dic['mixer']
    note = metadata_dic['note']

    x_axis_data = qepro_dic['QEPro_x_axis']
    dark_data = qepro_dic['QEPro_dark']
    reference_data = qepro_dic['QEPro_reference']
    sample_data = qepro_dic['QEPro_sample']
    output_data = qepro_dic['QEPro_output']
    
    if spectrum_type == 3:
        spec = 'Abs'
        fout = f'{csv_path}/{sample_type}_{spec}_{date}-{time}_{uid[0:8]}.csv'
        
    if spectrum_type == 2:
        spec = 'PL'
        fout = f'{csv_path}/{sample_type}_{spec}_{date}-{time}_{uid[0:8]}.csv'

    with open(fout, 'w') as fp:
        fp.write(f'uid,{full_uid}\n')
        fp.write(f'Time_QEPro,{date},{time}\n')
        fp.write(f'Integration time (ms),{int_time}\n')
        fp.write(f'Number of averaged spectra,{num_average}\n')
        fp.write(f'Boxcar width,{boxcar_width}\n')

        for i in range(len(pump_names)):
            fp.write(f'{pump_names[i]},{precursor[i]},{infuse_rate[i]},{infuse_rate_unit[i]},{pump_status[i]}\n')
    
        if mixer != None:
            for i in range(len(mixer)):
                fp.write(f'Mixer no. {i+1},{mixer[i]}\n')

        if type(note) is str:
            fp.write(f'Note,{note}\n')

        if spectrum_type == 3:
            fp.write('Wavelength,Dark,Reference,Sample,Absorbance\n')
        else:
            fp.write('Wavelength,Dark,Sample,Fluorescence\n')

        for i in range(len(output_data)):
            if spectrum_type == 3:
                fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
            else:
                fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')

