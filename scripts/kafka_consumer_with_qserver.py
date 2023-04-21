import os
import datetime
import pprint
import uuid
# from bluesky_kafka import RemoteDispatcher
from bluesky_kafka.consume import BasicConsumer
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
import databroker

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

## Input varaibales
## Maybe they can be read from a .txt file in future
csv_path = '/home/xf28id2/Documents/ChengHung/20230421_qserver_device_on'
key_height = 200
height = 50
distance = 100
# pump_list = [dds1_p1.name, dds1_p2.name]
pump_list = ['dds1_p1', 'dds1_p2']
syringe_list = [50, 50]
target_vol_list = ['30 ml', '30 ml']
infuse_rates = [['100 ul/min', '100 ul/min'], ['200 ul/min', '200 ul/min'], ['50 ul/min', '50 ul/min']]
precursor_list = ['CsPbOA', 'ToABr']
mixer = ['30 cm']
syringe_mater_list=['steel', 'steel']
sample = ['CsPbBr_100ul', 'CsPbBr_200ul', 'CsPbBr_50ul']


def print_kafka_messages(beamline_acronym, csv_path=csv_path, 
                         key_height=key_height, height=height, distance=distance, 
                         pump_list=pump_list, sample=sample, precursor_list=precursor_list, mixer=mixer):
    print(f"Listening for Kafka messages for {beamline_acronym}")
    print(f'Defaul parameters:\n'
          f'                  csv path: {csv_path}\n'
          f'                  key height: {key_height}\n'
          f'                  height: {height}\n'
          f'                  distance: {distance}\n')


    global db, catalog
    db = databroker.Broker.named(beamline_acronym)
    catalog = databroker.catalog[f'{beamline_acronym}']


    # plt.figure()
    # def print_message(name, doc):
    def print_message(consumer, doctype, doc, 
                      bad_data = [], good_data = [], finished = [], 
                      pump_list=pump_list, sample=sample, precursor_list=precursor_list, mixer=mixer):
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
            # print('Kafka test good!!')
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
            
            ## remove 'scattering' from stream_list to avoid redundant work in next for loop
            if 'scattering' in stream_list:
                stream_list.remove('scattering')
            
            ## Export, plotting, fitting, calculate # of good/bad data, add queue item
            for stream_name in stream_list:
                ## Read data from databroker and turn into dic
                qepro_dic, metadata_dic = de.read_qepro_by_stream(uid, stream_name=stream_name, data_agent='tiled')
                ## Save data in dic into .csv file
                de.dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name)
                print(f'\n** export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(csv_path)} **\n')
                ## Plot data in dic
                u = plot_uvvis(qepro_dic, metadata_dic)
                u.plot_data()
                print(f'\n** Plot {stream_name} in uid: {uid[0:8]} complete **\n')
                    
                ## Idenfify good/bad data if it is a fluorescence sacn in 'primary'
                if qepro_dic['QEPro_spectrum_type'][0]==2 and stream_name=='primary':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    x0, y0, data_id, peak, prop = da._identify_one_in_kafka(qepro_dic, metadata_dic, key_height=key_height, distance=distance, height=height)
                
                ## Avergae scans in 'fluorescence' and idenfify good/bad
                elif stream_name == 'fluorescence':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    x0, y0, data_id, peak, prop = da._identify_multi_in_kafka(qepro_dic, metadata_dic, key_height=key_height, distance=distance, height=height)
                    
                try:
                    data_id, peak, prop
                    ## for a good data, type(peak) will be a np.array and type(prop) will be a dic
                    ## fit the good data, export/plotting fitting results
                    ## append data_id into good_data or bad_data for calculate numbers
                    if (type(peak) is np.ndarray) and (type(prop) is dict):
                        x, y, p, f, popt = da._fitting_in_kafka(x0, y0, data_id, peak, prop)                       
                        ff={'fit_function': f, 'curve_fit': popt}
                        de.dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name, fitting=ff)
                        print(f'\n** export fitting results complete**\n')
                        
                        u.plot_peak_fit(x, y, p, f, popt, fill_between=True)
                        print(f'\n** plot fitting results complete**\n')
                        if stream_name == 'primary':
                            good_data.append(data_id)
                    
                    elif peak==[] and prop==[]:
                        bad_data.append(data_id)

                except (NameError, TypeError):
                    print(f"\n*** No need to carry out fitting for {stream_name} in uid: {uid[:8]} ***\n")
            
            print('\n*** export, identify good/bad, fitting complete ***\n')
            print(f'*** Accumulated num of good data: {len(good_data)} ***\n')
            print(f'good_data = {good_data}\n')
            print(f'*** Accumulated num of bad data: {len(bad_data)} ***\n')
            print('########### Events printing division ############\n')
            
            
            ## Depend on # of good/bad data, add items into queue item or stop 
            if stream_name == 'primary':     
                if len(bad_data) > 5:
                    print('*** qsever aborted due to too many bad scans, please check setup ***\n')
                    zmq_single_request(method='queue_stop')
                    # zmq_single_request(method='re_abort')
                    
                elif len(good_data) <= 5:
                    print('*** Add another fluorescence scan to the fron of qsever ***\n')
                    
                    zmq_single_request(method='queue_item_add', 
                                    params={
                                            'item':{"name":"sleep_sec_q", 
                                                        "args":[5], 
                                                        "item_type":"plan"
                                                    }, 'pos':'front', 'user_group':'primary', 'user':'chlin'})
                    
                    zmq_single_request(method='queue_item_add', 
                                    params={
                                            'item':{"name":"take_a_uvvis_csv_q",  
                                                "kwargs": {'sample_type':sample[len(finished)], 
                                                        'spectrum_type':'Corrected Sample', 'correction_type':'Dark', 
                                                        'pump_list':pump_list, 'precursor_list':precursor_list, 
                                                            'mixer':mixer
                                                            }, "item_type":"plan"
                                                    }, 'pos':'front', 'user_group':'primary', 'user':'chlin'})

                elif len(good_data) > 5:
                    print('*** # of good data is enough so go to the next: bundle plan ***\n')
                    bad_data.clear()
                    good_data.clear()
                    finished.append(metadata_dic['sample_type'])
                    print(f'After event: good_data = {good_data}\n')
                    print(f'After event: finished sample = {finished}\n')
                    
                    # zmq_single_request(method='queue_start')


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
