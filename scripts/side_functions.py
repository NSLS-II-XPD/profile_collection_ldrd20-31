import datetime
import matplotlib.pyplot as plt
import time
import numpy as np
import pandas as pd

def _readable_time(unix_time):
    from datetime import datetime
    dt = datetime.fromtimestamp(unix_time)
    print(f'{dt.year}{dt.month:02d}{dt.day:02d},{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')
    return (f'{dt.year}{dt.month:02d}{dt.day:02d}'), (f'{dt.hour:02d}{dt.minute:02d}{dt.second:02d}')


### the export funs below are revised from self.export_from_scan in 10-QEPro.py
def export_qepro_from_uid(uid, csv_path, sample_type=None, plot=False, data_agent='db', wait=False):
    if wait==True:
        time.sleep(2)
    
    if data_agent == 'db':
        qepro_dic, metadata_dic = read_qepro_from_db(uid)
        dic_to_csv(csv_path, qepro_dic, metadata_dic)
    elif data_agent == 'tiled':
        qepro_dic, metadata_dic = read_qepro_from_tiled(uid)
        dic_to_csv(csv_path, qepro_dic, metadata_dic)

    

def read_qepro_from_db(uid):
    try:
        db
    except NameError:
        import databroker
        db = databroker.Broker.named('xpd-ldrd20-31')
    
    # full_uid = db[uid].start['uid']
    # unix_time = db[uid].start['time']     
    # date, time = _readable_time(unix_time)

    qepro_list=['QEPro_x_axis', 'QEPro_output', 'QEPro_sample', 'QEPro_dark', 'QEPro_reference', 
                'QEPro_spectrum_type', 'QEPro_integration_time', 'QEPro_num_spectra', 'QEPro_buff_capacity']
    qepro_dic = {}
    for i in qepro_list:
        qepro_dic[i] = db[uid].table()[i][1]

    metadata_list=['uid', 'time', 'pumps','precursors','infuse_rate','infuse_rate_unit',
                   'pump_status', 'mixer','sample_type']
    metadata_dic = {}
    for i in metadata_list:
        if i in db[uid].start.keys():
            metadata_dic[i] = db[uid].start[i]
        else:
            metadata_dic[i] = [None]
    
    return qepro_dic, metadata_dic


def read_qepro_from_tiled(uid):
    try:
        tiled.client
    except NameError:
        from tiled.client import from_profile
        tiled_client = from_profile("xpd-ldrd20-31")  
    
    run = tiled_client[uid]
    ds = run.primary.read()
    meta = run.metadata
    
    # date, time = _readable_time(ds['time'][0])
    # full_uid = meta['start']['uid']
    
    qepro_list=['QEPro_x_axis', 'QEPro_output', 'QEPro_sample', 'QEPro_dark', 'QEPro_reference', 
                'QEPro_spectrum_type', 'QEPro_integration_time', 'QEPro_num_spectra', 'QEPro_buff_capacity']
    qepro_dic = {}
    for i in qepro_list:
        qepro_dic[i] = ds[i].values[0]

    metadata_list=['uid', 'time', 'pumps','precursors','infuse_rate','infuse_rate_unit',
                   'pump_status', 'mixer','sample_type']
    metadata_dic = {}
    for i in metadata_list:
        if i in meta['start'].keys():
            metadata_dic[i] = meta['start'][i]
        # else:
        #     metadata_dic[i] = [None]
    
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
    precursor_list = metadata_dic['precursors']
    infuse_rate = metadata_dic['infuse_rate']
    infuse_rate_unit = metadata_dic['infuse_rate_unit']
    pump_status = metadata_dic['pump_status']
    mixer = metadata_dic['mixer']

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

        if spectrum_type == 3:
            fp.write('Wavelength,Dark,Reference,Sample,Absorbance\n')
        else:
            fp.write('Wavelength,Dark,Sample,Fluorescence\n')

        for i in range(len(output_data)):
            if spectrum_type == 3:
                fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
            else:
                fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')




def export_from_scan(uid, csv_path, sample_type=None, plot=False, data_agent='db', wait=False):
    if wait==True:
        time.sleep(2)
    
    if data_agent == 'db':      
        unix_time = db[uid].start['time']     
        date, time = _readable_time(unix_time)
        
        x_axis_data = db[uid].table().QEPro_x_axis[1]
        output_data = db[uid].table().QEPro_output[1]
        sample_data = db[uid].table().QEPro_sample[1]
        dark_data = db[uid].table().QEPro_dark[1]
        reference_data = db[uid].table().QEPro_reference[1]
        spectrum_type = db[uid].table().QEPro_spectrum_type[1]
        int_time = db[uid].table().QEPro_integration_time[1]
        num_average = db[uid].table().QEPro_num_spectra[1]
        boxcar_width = db[uid].table().QEPro_buff_capacity[1]
        
        full_uid = db[uid].start['uid']

        metadata_list=['pumps','precursors','infuse_rate','infuse_rate_unit','pump_status',
                        'mixer','sample_type']
        if 'pumps' in db[uid].start.keys():
            pump_names = db[uid].start['pumps']
        else: pump_names = ['None']
        if 'precursors' in db[uid].start.keys():
            precursor = db[uid].start['precursors']
        else: precursor = ['None']
        if 'infuse_rate' in db[uid].start.keys():
            infuse_rate = db[uid].start['infuse_rate']
        else: infuse_rate = ['None']
        if 'infuse_rate_unit' in db[uid].start.keys():
            infuse_rate_unit = db[uid].start['infuse_rate_unit']
        else: infuse_rate_unit = ['None']
        if 'pump_status' in db[uid].start.keys():
            pump_status = db[uid].start['pump_status']
        else: pump_status = ['None']
        if 'mixer' in db[uid].start.keys():
            mixer = db[uid].start['mixer']
        else: mixer = ['None']
        if 'sample_type' in db[uid].start.keys():
            sample_type = db[uid].start['sample_type']
        # else: sample_type = None
        

    if data_agent == 'tiled':    
        run = tiled_client[uid]
        ds = run.primary.read()
        meta = run.metadata
        
        date, time = _readable_time(ds['time'][0])
        x_axis_data = ds['QEPro_x_axis'].values[0]
        output_data = ds['QEPro_output'].values[0]
        sample_data = ds['QEPro_sample'].values[0]
        dark_data = ds['QEPro_dark'].values[0]
        reference_data = ds['QEPro_reference'].values[0]
        spectrum_type = ds['QEPro_spectrum_type'].values[0]
        int_time = ds['QEPro_integration_time'].values[0]
        num_average = ds['QEPro_num_spectra'].values[0]
        boxcar_width = ds['QEPro_buff_capacity'].values[0]
        
        full_uid = meta['start']['uid']

        if 'pumps' in meta['start'].keys():
            pump_names = meta['start']['pumps']
        else: pump_names = ['None']
        if 'precursors' in meta['start'].keys():
            precursor = meta['start']['precursors']
        else: precursor = ['None']
        if 'infuse_rate' in meta['start'].keys():
            infuse_rate = meta['start']['infuse_rate']
        else: infuse_rate = ['None']
        if 'infuse_rate_unit' in meta['start'].keys():
            infuse_rate_unit = meta['start']['infuse_rate_unit']
        else: infuse_rate_unit = ['None']
        if 'pump_status' in meta['start'].keys():
            pump_status = meta['start']['pump_status']
        else: pump_status = ['None']
        if 'mixer' in meta['start'].keys():
            mixer = meta['start']['mixer']
        else: mixer = ['None']
        if 'sample_type' in meta['start'].keys():
            sample_type = meta['start']['sample_type']
        # else: sample_type = None
            
    
    if plot == True:
        # x_axis_label = self.x_axis_format.get(as_string=True)
        x_axis_label = 'Wavelength (nm)'
        if spectrum_type == 3:
            y_axis_label = 'Absorbance'
        elif spectrum_type == 2:
            y_axis_label = 'Fluorescence'
        plt.figure()
        plt.plot(x_axis_data, output_data)
        plt.xlabel(x_axis_label)
        plt.ylabel(y_axis_label)
        plt.show()

    if csv_path != None:
        
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

            if spectrum_type == 3:
                fp.write('Wavelength,Dark,Reference,Sample,Absorbance\n')
            else:
                fp.write('Wavelength,Dark,Sample,Fluorescence\n')

            for i in range(len(output_data)):
                if spectrum_type == 3:
                    fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
                else:
                    fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')