import os
import datetime
import pprint
import uuid
# from bluesky_kafka import RemoteDispatcher
from bluesky_kafka.consume import BasicConsumer
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import integrate
import time
import databroker
import json
import glob
from tqdm import tqdm

import resource
resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da

from bluesky_queueserver.manager.comms import zmq_single_request

# db = databroker.Broker.named('xpd-ldrd20-31')
# catalog = databroker.catalog['xpd-ldrd20-31']

try:
    from nslsii import _read_bluesky_kafka_config_file  # nslsii <0.7.0
except (ImportError, AttributeError):
    from nslsii.kafka_utils import _read_bluesky_kafka_config_file  # nslsii >=0.7.0

# these two lines allow a stale plot to remain interactive and prevent
# the current plot from stealing focus.  thanks to Tom:
# https://nsls2.slack.com/archives/C02D9V72QH1/p1674589090772499
plt.ion()
plt.rcParams["figure.raise_window"] = False

## Input varaibales: read from inputs_qserver_kafka.xlsx
xlsx = '/home/xf28id2/Documents/ChengHung/inputs_qserver_kafka_ML.xlsx'
input_dic = de._read_input_xlsx(xlsx, sheet_name='inputs_xrun')

##################################################################
# Define namespace for tasks in Qserver and Kafa
dummy_kafka = bool(input_dic['dummy_test'][0])
dummy_qserver = bool(input_dic['dummy_test'][1])
csv_path = input_dic['csv_path'][0]
key_height = input_dic['key_height']
height = input_dic['height']
distance = input_dic['distance']
pump_list = input_dic['pump_list']
precursor_list = input_dic['precursor_list']
syringe_mater_list = input_dic['syringe_mater_list']
syringe_list = input_dic['syringe_list']
target_vol_list = input_dic['target_vol_list']
set_target_list = input_dic['set_target_list']
infuse_rates = input_dic['infuse_rates']
sample = input_dic['sample']
mixer = input_dic['mixer']
wash_tube = input_dic['wash_tube']
resident_t_ratio = input_dic['resident_t_ratio']
PLQY = input_dic['PLQY']
prefix = input_dic['prefix']
num_uvvis = input_dic['num_uvvis']
###################################################################
## Add tasks into Qsever
zmq_server_address = 'tcp://xf28id2-ca2:60615'
import _synthesis_queue_RM as sq
sq.synthesis_queue(
                    syringe_list=syringe_list, 
                    pump_list=pump_list, 
                    set_target_list=set_target_list, 
                    target_vol_list=target_vol_list, 
                    rate_list = infuse_rates, 
                    syringe_mater_list=syringe_mater_list, 
                    precursor_list=precursor_list,
                    mixer=mixer, 
                    resident_t_ratio=resident_t_ratio, 
                    prefix=prefix[1:], 
                    sample=sample, 
                    wash_tube=wash_tube, 
                    name_by_prefix=bool(prefix[0]),  
					num_abs=num_uvvis[0], 
					num_flu=num_uvvis[1], 
                    
                    )

if bool(prefix[0]):
    sample = de._auto_name_sample(infuse_rates, prefix=prefix[1:])
print(f'Sample: {sample}')

# import sys
# sys.path.insert(0, "/home/xf28id2/src/blop/blop")
# from bloptools.bayesian import Agent, DOF, Objective

from blop import Agent, DOF, Objective



# agent_data_path = '/home/xf28id2/data_ZnCl2'
# agent_data_path = '/home/xf28id2/data_ZnI2_60mM'
agent_data_path = '/home/xf28id2/data_halide/'
# agent_data_path = '/home/xf28id2/data_dilute_halide'

# dofs = [
#     DOF(description="CsPb(oleate)3", name="infusion_rate_CsPb", units="uL/min", search_domain=(5, 110)),
#     DOF(description="TOABr", name="infusion_rate_Br", units="uL/min", search_domain=(70, 170)),
#     DOF(description="ZnCl2", name="infusion_rate_Cl", units="uL/min", search_domain=(0, 150)),
#     DOF(description="ZnI2", name="infusion_rate_I2", units="uL/min", search_domain=(0, 150)),
# ]

# objectives = [
#     Objective(description="Peak emission", name="Peak", target=660, weight=10, max_noise=0.25),
#     Objective(description="Peak width", name="FWHM", target="min", transform="log", weight=2., max_noise=0.25),
#     Objective(description="Quantum yield", name="PLQY", target="max", transform="log", weight=1., max_noise=0.25),
# ]

use_good_bad = True
USE_AGENT_iterate = True
peak_target = 470

write_agent_data = False
# rate_label = ['infusion_rate_CsPb', 'infusion_rate_Br', 'infusion_rate_Cl', 'infusion_rate_I2']
# rate_label = ['infusion_rate_CsPb', 'infusion_rate_Br', 'infusion_rate_I2', 'infusion_rate_Cl']
rate_label_dic =   {'CsPb':'infusion_rate_CsPb', 
                    'Br':'infusion_rate_Br', 
                    'ZnI':'infusion_rate_I2', 
                    'ZnCl':'infusion_rate_Cl'}

if USE_AGENT_iterate:

    # from prepare_agent import build_agen
    from prepare_agent_pdf import build_agen_Cl
    agent = build_agen_Cl(peak_target=peak_target)




def print_kafka_messages(beamline_acronym, csv_path=csv_path, 
                         key_height=key_height, height=height, distance=distance, 
                         pump_list=pump_list, sample=sample, precursor_list=precursor_list, 
                         mixer=mixer, dummy_test=dummy_kafka, plqy=PLQY, prefix=prefix, 
                         agent_data_path=agent_data_path, peak_target=peak_target, 
                         rate_label_dic=rate_label_dic):

    print(f"Listening for Kafka messages for {beamline_acronym}")
    print(f'Defaul parameters:\n'
          f'                  csv path: {csv_path}\n'
          f'                  key height: {key_height}\n'
          f'                  height: {height}\n'
          f'                  distance: {distance}\n'
          f'{use_good_bad = }\n'
          f'{USE_AGENT_iterate = }\n'
          f'{peak_target = }\n'
          f'{write_agent_data = }\n'
          f'{zmq_server_address = }')


    global db, catalog
    db = databroker.Broker.named(beamline_acronym)
    catalog = databroker.catalog[f'{beamline_acronym}']

    import palettable.colorbrewer.diverging as pld
    palette = pld.RdYlGn_4_r
    cmap = palette.mpl_colormap
    color_idx = np.linspace(0, 1, len(sample))

    # plt.figure()
    # def print_message(name, doc):
    def print_message(consumer, doctype, doc, 
                      bad_data = [], good_data = [], check_abs365 = False, finished = [], 
                      agent_iteration = [], ):
                    #   pump_list=pump_list, sample=sample, precursor_list=precursor_list, 
                    #   mixer=mixer, dummy_test=dummy_test):
        name, message = doc
        # print(
        #     f"{datetime.datetime.now().isoformat()} document: {name}\n"
        #     f"document keys: {list(message.keys())}\n"
        #     f"contents: {pprint.pformat(message)}\n"
        # )
        if name == 'start':
            print(
                f"{datetime.datetime.now().isoformat()} documents {name}\n"
                f"document keys: {list(message.keys())}")
                
            if 'uid' in message.keys():
                print(f"uid: {message['uid']}")
            if 'plan_name' in message.keys():
                print(f"plan name: {message['plan_name']}")
            if 'detectors' in message.keys(): 
                print(f"detectors: {message['detectors']}")
            if 'pumps' in message.keys(): 
                print(f"pumps: {message['pumps']}")
            # if 'detectors' in message.keys(): 
            #     print(f"detectors: {message['detectors']}")
            if 'uvvis' in message.keys() and message['plan_name']!='count':
                print(f"uvvis mode:\n"
                      f"           integration time: {message['uvvis'][0]} ms\n"
                      f"           num spectra averaged: {message['uvvis'][1]}\n"
                      f"           buffer capacity: {message['uvvis'][2]}"
                      )
            elif 'uvvis' in message.keys() and message['plan_name']=='count':
                print(f"uvvis mode:\n"
                      f"           spectrum type: {message['uvvis'][0]}\n"
                      f"           integration time: {message['uvvis'][2]} ms\n"
                      f"           num spectra averaged: {message['uvvis'][3]}\n"
                      f"           buffer capacity: {message['uvvis'][4]}"
                      )                
            if 'mixer' in message.keys():
                print(f"mixer: {message['mixer']}")
            if 'sample_type' in message.keys():
                print(f"sample type: {message['sample_type']}")
            
        if name == 'stop':
            zmq_single_request(method='queue_stop', zmq_server_address=zmq_server_address)
            print('\n*** qsever stop for data export, identification, and fitting ***\n')

            print(f"{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(message)}"
            )
            # num_events = len(message['num_events'])

            ## wait 2 seconds for databroker to save data
            time.sleep(2)
            uid = message['run_start']
            print(f'\n**** start to export uid: {uid} ****\n')
            # print(list(message['num_events'].keys())[0])
            stream_list = list(message['num_events'].keys())

            ## Set good/bad data condictions to the corresponding sample
            try:
                kh = key_height[len(finished)]
                hei = height[len(finished)]
                dis = distance[len(finished)]
            except IndexError:
                kh = key_height[0]
                hei = height[0]
                dis = distance[0]
            
            ## remove 'scattering' from stream_list to avoid redundant work in next for loop
            if 'scattering' in stream_list:
                stream_list.remove('scattering')
            
            ## Export, plotting, fitting, calculate # of good/bad data, add queue item
            for stream_name in stream_list:
                ## Read data from databroker and turn into dic
                qepro_dic, metadata_dic = de.read_qepro_by_stream(uid, stream_name=stream_name, data_agent='tiled')
                sample_type = metadata_dic['sample_type']
                ## Save data in dic into .csv file
                de.dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name)
                print(f'\n** export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(csv_path)} **\n')
                ## Plot data in dic
                u = plot_uvvis(qepro_dic, metadata_dic)
                if len(good_data)==0 and len(bad_data)==0:
                    clear_fig=True
                else:
                    clear_fig=False
                u.plot_data(clear_fig=clear_fig)
                print(f'\n** Plot {stream_name} in uid: {uid[0:8]} complete **\n')
                    
                ## Idenfify good/bad data if it is a fluorescence sacn in 'primary'
                if qepro_dic['QEPro_spectrum_type'][0]==2 and stream_name=='primary':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    x0, y0, data_id, peak, prop = da._identify_one_in_kafka(qepro_dic, metadata_dic, key_height=kh, distance=dis, height=hei, dummy_test=dummy_test)
                
                
                ## Apply an offset to zero baseline of absorption spectra
                elif stream_name == 'absorbance':
                    print(f'\n*** start to check absorbance at 365b nm in stream: {stream_name} is positive or not***\n')
                    abs_array = qepro_dic['QEPro_output'][1:].mean(axis=0)
                    wavelength = qepro_dic['QEPro_x_axis'][0]

                    popt_abs01, _ = da.fit_line_2D(wavelength, abs_array, da.line_2D, x_range=[205, 240], plot=False)
                    popt_abs02, _ = da.fit_line_2D(wavelength, abs_array, da.line_2D, x_range=[750, 950], plot=False)
                    if abs(popt_abs01[0]) >= abs(popt_abs02[0]):
                        popt_abs = popt_abs02
                    elif abs(popt_abs01[0]) <= abs(popt_abs02[0]):
                        popt_abs = popt_abs01
                    
                    abs_array_offset = abs_array - da.line_2D(wavelength, *popt_abs)

                    print(f'\nFitting function for baseline offset: {da.line_2D}\n')
                    ff_abs={'fit_function': da.line_2D, 'curve_fit': popt_abs}
                    de.dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name, fitting=ff_abs)
                    u.plot_offfset(wavelength, da.line_2D, popt_abs)
                    print(f'\n** export offset results of absorption spectra complete**\n')

                
                ## Avergae scans in 'fluorescence' and idenfify good/bad
                elif stream_name == 'fluorescence':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    x0, y0, data_id, peak, prop = da._identify_multi_in_kafka(qepro_dic, metadata_dic, key_height=kh, distance=dis, height=hei, dummy_test=dummy_test)
                    label_uid = f'{uid[0:8]}_{metadata_dic["sample_type"]}'
                    # u.plot_average_good(x0, y0, color=cmap(color_idx[sub_idx]), label=label_uid)
                    # sub_idx = sample.index(metadata_dic['sample_type'])
                    u.plot_average_good(x0, y0, label=label_uid, clf_limit=9)
                    
                ## Skip peak fitting if qepro type is absorbance
                if qepro_dic['QEPro_spectrum_type'][0] == 3:  
                    print(f"\n*** No need to carry out fitting for {stream_name} in uid: {uid[:8]} ***\n")

                else: 
                    ## for a good data, type(peak) will be a np.array and type(prop) will be a dic
                    ## fit the good data, export/plotting fitting results
                    ## append data_id into good_data or bad_data for calculate numbers
                    if (type(peak) is np.ndarray) and (type(prop) is dict):
                        x, y, p, f_fit, popt = da._fitting_in_kafka(x0, y0, data_id, peak, prop, dummy_test=dummy_test)    

                        fitted_y = f_fit(x, *popt)
                        r2_idx1, _ = da.find_nearest(x, popt[1] - 3*popt[2])
                        r2_idx2, _ = da.find_nearest(x, popt[1] + 3*popt[2])
                        r_2 = da.r_square(x[r2_idx1:r2_idx2], y[r2_idx1:r2_idx2], fitted_y[r2_idx1:r2_idx2], y_low_limit=0)               

                        metadata_dic["r_2"] = r_2                                   
                        
                        if 'gauss' in f_fit.__name__:
                            constant = 2.355
                        else:
                            constant = 1

                        intensity_list = []
                        peak_list = []
                        fwhm_list = []
                        for i in range(int(len(popt)/3)):
                            intensity_list.append(popt[i*3+0])
                            peak_list.append(popt[i*3+1])
                            fwhm_list.append(popt[i*3+2]*constant)
                        
                        peak_emission_id = np.argmax(np.asarray(intensity_list))
                        peak_emission = peak_list[peak_emission_id]
                        fwhm = fwhm_list[peak_emission_id]

                        ## Calculate PLQY for fluorescence stream
                        if (stream_name == 'fluorescence') and (PLQY[0]==1):
                            PL_integral_s = integrate.simpson(y,x)
                            label_uid = f'{uid[0:8]}_{metadata_dic["sample_type"]}'
                            u.plot_CsPbX3(x, y, peak_emission, label=label_uid, clf_limit=9)
                            
                            ## Find absorbance at 365 nm from absorbance stream
                            # q_dic, m_dic = de.read_qepro_by_stream(uid, stream_name='absorbance', data_agent='tiled')
                            # abs_array = q_dic['QEPro_output'][1:].mean(axis=0)
                            # wavelength = q_dic['QEPro_x_axis'][0]
                            
                            idx1, _ = da.find_nearest(wavelength, PLQY[2])
                            absorbance_s = abs_array_offset[idx1]

                            if PLQY[1] == 'fluorescein':
                                plqy = da.plqy_fluorescein(absorbance_s, PL_integral_s, 1.506, *PLQY[3:])
                            else:
                                plqy = da.plqy_quinine(absorbance_s, PL_integral_s, 1.506, *PLQY[3:])

                            
                            plqy_dic = {'PL_integral':PL_integral_s, 'Absorbance_365':absorbance_s, 'plqy': plqy}
                            
                            optical_property = {'Peak': peak_emission, 'FWHM':fwhm, 'PLQY':plqy}

                            if write_agent_data:
                                agent_data = {}

                                agent_data.update(optical_property)
                                agent_data.update({k:v for k, v in metadata_dic.items() if len(np.atleast_1d(v)) == 1})

                                agent_data = de._exprot_rate_agent(metadata_dic, rate_label_dic, agent_data)
                                                                
                                with open(f"{agent_data_path}/{data_id}.json", "w") as f:
                                    json.dump(agent_data, f)

                                print(f"\nwrote to {agent_data_path}")

                            
                            ### Three parameters for ML: peak_emission, fwhm, plqy
                            # TODO: add ML agent code here


                            if USE_AGENT_iterate:

                                # print(f"\ntelling agent {agent_data}")
                                # agent.tell(data=agent_data, append=True)

                                agent = build_agen_Cl(peak_target=peak_target)

                                if len(agent.table) < 2:
                                    acq_func = "qr"
                                else:
                                    acq_func = "qei"
                                
                                # agent._construct_all_models()
                                # agent._train_all_models()
                                # new_points, _ = agent.ask(acq_func, n=1)
                                new_points = agent.ask(acq_func, n=1)
                                # new_points = agent.ask("qem", n=1)
                                # agent.save_data(filepath="/home/xf28id2/data_halide/init_240122_01.h5")

                                # peak_diff = peak_emission - peak_target
                                peak_diff = False

                                # if (peak_diff <= 3) and (peak_diff >=-3):
                                if peak_diff:
                                    print(f'\nTarget peak: {peak_target} nm vs. Current peak: {peak_emission} nm\n')
                                    print(f'\nReach the target, stop iteration, stop all pumps, and wash the loop.\n')

                                    ### Stop all infusing pumps
                                    zmq_single_request(
                                        method='queue_item_add', 
                                        zmq_server_address=zmq_server_address, 
                                        params={
                                            'item':{
                                                "name":"stop_group", 
                                                "args": [pump_list],  
                                                "item_type":"plan"}, 
                                            'pos': 'front', 
                                            'user_group':'primary', 
                                            'user':'chlin'})

                                    ### Set up washing tube/loop
                                    zmq_single_request(
                                        method='queue_item_add', 
                                        zmq_server_address=zmq_server_address, 
                                        params={
                                            'item':{
                                                "name":"start_group_infuse", 
                                                "args": [[wash_tube[1]], [wash_tube[2]]],  
                                                "item_type":"plan"}, 
                                            'pos': 1, 
                                            'user_group':'primary', 
                                            'user':'chlin'})

                                    ### Wash loop/tube for xxx seconds
                                    zmq_single_request(
                                        method='queue_item_add', 
                                        zmq_server_address=zmq_server_address, 
                                        params={
                                            'item':{
                                                "name":"sleep_sec_q", 
                                                "args":[wash_tube[3]], 
                                                "item_type":"plan"}, 
                                            'pos': 2, 
                                            'user_group':'primary', 
                                            'user':'chlin'})


                                    ### Stop washing
                                    zmq_single_request(
                                        method='queue_item_add', 
                                        zmq_server_address=zmq_server_address, 
                                        params={
                                            'item':{
                                                "name":"stop_group", 
                                                "args": [[wash_tube[1]]],  
                                                "item_type":"plan"}, 
                                            'pos': 3, 
                                            'user_group':'primary', 
                                            'user':'chlin'})
                                
                                    zmq_single_request(method='queue_stop', zmq_server_address=zmq_server_address)
                                    # zmq_single_request(method='re_abort')
                                    agent_iteration.append(False)
                                
                                else:
                                    agent_iteration.append(True)

                        else:
                            plqy_dic = None
                            optical_property = None
                        
                        print(f'\nFitting function: {f_fit}\n')
                        ff={'fit_function': f_fit, 'curve_fit': popt}
                        de.dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name, fitting=ff, plqy_dic=plqy_dic)
                        print(f'\n** export fitting results complete**\n')
                        
                        u.plot_peak_fit(x, y, f_fit, popt, peak=p, fill_between=True)
                        print(f'\n** plot fitting results complete**\n')
                        if stream_name == 'primary':
                            good_data.append(data_id)
                    
                    elif peak==[] and prop==[]:
                        bad_data.append(data_id)
                        print(f"\n*** No need to carry out fitting for {stream_name} in uid: {uid[:8]} ***\n")
                        print(f"\n*** since {stream_name} in uid: {uid[:8]} is a bad data.***\n")
            
                    print('\n*** export, identify good/bad, fitting complete ***\n')
                    
                    try :
                        print(f"\n*** {sample_type} of uid: {uid[:8]} has: {optical_property}.***\n")
                    except (UnboundLocalError):
                        pass

            print(f'*** Accumulated num of good data: {len(good_data)} ***\n')
            print(f'good_data = {good_data}\n')
            print(f'*** Accumulated num of bad data: {len(bad_data)} ***\n')
            print('########### Events printing division ############\n')
            
            
            ## Depend on # of good/bad data, add items into queue item or stop 
            if stream_name == 'primary' and use_good_bad:     
                if len(bad_data) > 3:
                    print('*** qsever aborted due to too many bad scans, please check setup ***\n')

                    ### Stop all infusing pumps
                    zmq_single_request(
                        method='queue_item_add', 
                        zmq_server_address=zmq_server_address, 
                        params={
                            'item':{
                                "name":"stop_group", 
                                "args": [pump_list],  
                                "item_type":"plan"}, 
                            'pos': 'front', 
                            'user_group':'primary', 
                            'user':'chlin'})

                    ### Set up washing tube/loop
                    zmq_single_request(
                        method='queue_item_add', 
                        zmq_server_address=zmq_server_address, 
                        params={
                            'item':{
                                "name":"start_group_infuse", 
                                "args": [[wash_tube[1]], [wash_tube[2]]],  
                                "item_type":"plan"}, 
                            'pos': 1, 
                            'user_group':'primary', 
                            'user':'chlin'})

                    # from bluesky_queueserver_api.zmq import REManagerAPI
                    # from bluesky_queueserver_api import BPlan
                    # RM = REManagerAPI( < address etc. >)
                    # plan = BPlan("start_group_infuse", [wash_tube[1]], [wash_tube[2]])
                    # plan.meta = ...
                    # RM.item_add(plan)
                    
                    ### Wash loop/tube for xxx seconds
                    zmq_single_request(
                        method='queue_item_add', 
                        zmq_server_address=zmq_server_address, 
                        params={
                            'item':{
                                "name":"sleep_sec_q", 
                                "args":[wash_tube[3]], 
                                "item_type":"plan"}, 
                            'pos': 2, 
                            'user_group':'primary', 
                            'user':'chlin'})


                    ### Stop washing
                    zmq_single_request(
                        method='queue_item_add', 
                        zmq_server_address=zmq_server_address, 
                        params={
                            'item':{
                                "name":"stop_group", 
                                "args": [[wash_tube[1]]],  
                                "item_type":"plan"}, 
                            'pos': 3, 
                            'user_group':'primary', 
                            'user':'chlin'})
                   
                    zmq_single_request(method='queue_stop', zmq_server_address=zmq_server_address)
                    # zmq_single_request(method='re_abort')
                    
                elif len(good_data) <= 2 and use_good_bad:
                    print('*** Add another fluorescence and absorption scan to the fron of qsever ***\n')

                    zmq_single_request(
                        method='queue_item_add', 
                        zmq_server_address=zmq_server_address, 
                        params={
                            'item':{
                                "name":"take_a_uvvis_csv_q",  
                                "kwargs": {
                                    'sample_type':metadata_dic['sample_type'], 
                                    'spectrum_type':'Corrected Sample', 
                                    'correction_type':'Dark', 
                                    'pump_list':pump_list, 
                                    'precursor_list':precursor_list, 
                                    'mixer':mixer}, 
                                "item_type":"plan"}, 
                            'pos':'front', 
                            'user_group':'primary', 
                            'user':'chlin'})
                    
                    zmq_single_request(method='queue_start', zmq_server_address=zmq_server_address)

                elif len(good_data) > 2 and use_good_bad:
                    print('*** # of good data is enough so go to the next: bundle plan ***\n')
                    bad_data.clear()
                    good_data.clear()
                    finished.append(metadata_dic['sample_type'])
                    print(f'After event: good_data = {good_data}\n')
                    print(f'After event: finished sample = {finished}\n')
                    zmq_single_request(method='queue_start', zmq_server_address=zmq_server_address)
            
            elif stream_name == 'fluorescence' and USE_AGENT_iterate and agent_iteration[-1]:
                print('*** Add new points from agent to the fron of qsever ***\n')
                print(f'*** New points from agent: {new_points} ***\n')
                
                set_target_list = [0 for i in range(new_points['points'].shape[1])]
                sample = de._auto_name_sample(new_points['points'], prefix=prefix[1:])

                sq.synthesis_queue(
                    syringe_list=syringe_list, 
                    pump_list=pump_list, 
                    set_target_list=set_target_list, 
                    target_vol_list=target_vol_list, 
                    rate_list = new_points['points'], 
                    syringe_mater_list=syringe_mater_list, 
                    precursor_list=precursor_list,
                    mixer=mixer, 
                    resident_t_ratio=resident_t_ratio, 
                    prefix=prefix[1:], 
                    sample=sample, 
                    wash_tube=wash_tube, 
                    name_by_prefix=bool(prefix[0]),  
					num_abs=num_uvvis[0], 
					num_flu=num_uvvis[1], 
                    is_iteration=True, 
                    zmq_server_address=zmq_server_address, 
                    )

                zmq_single_request(method='queue_start', zmq_server_address=zmq_server_address)
    
            # elif use_good_bad:
            else:
                zmq_single_request(method='queue_start', zmq_server_address=zmq_server_address)


    kafka_config = _read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"echo-{beamline_acronym}-{str(uuid.uuid4())[:8]}"

    kafka_consumer = BasicConsumer(
        topics=[f"{beamline_acronym}.bluesky.runengine.documents"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
        process_message = print_message,
    )

    try:
        kafka_consumer.start_polling(work_during_wait=lambda : plt.pause(.1))
    except KeyboardInterrupt:
        print('\nExiting Kafka consumer')
        return()


if __name__ == "__main__":
    import sys
    print_kafka_messages(sys.argv[1])
