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


from _data_export import read_qepro_by_stream, dic_to_csv_for_stream, _readable_time
from _plot_helper import plot_uvvis
import _data_analysis as da

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


def print_kafka_messages(beamline_acronym, csv_path):
    print(f"Listening for Kafka messages for {beamline_acronym}")

    global db, catalog
    db = databroker.Broker.named(beamline_acronym)
    catalog = databroker.catalog[f'{beamline_acronym}']

    # plt.figure()
    # def print_message(name, doc):
    def print_message(consumer, doctype, doc):
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

            time.sleep(2)
            uid = message['run_start']
            print(f'\n**** start to export uid: {uid} ****\n')
            for stream_name in ['primary', 'absorbance', 'fluorescence']:
                if stream_name in message['num_events'].keys():
                    qepro_dic, metadata_dic = read_qepro_by_stream(uid, stream_name=stream_name, data_agent='catalog')
                    dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name)
                    print(f'\n** export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(csv_path)} **\n')
                    u = plot_uvvis(qepro_dic, metadata_dic)
                    u.plot_data()
                    print(f'\n** Plot {stream_name} in uid: {uid[0:8]} complete **\n')
                    
                    print('\n*** start to identify good/bad data ***\n')
                    if qepro_dic['QEPro_spectrum_type'][0] == 2:
                        _, time1 = _readable_time(metadata_dic['time'])
                        data_id = time1 + '_' + metadata_dic['uid'][:8]
                        _for_average = pd.DataFrame()
                        for i in range(qepro_dic['QEPro_spectrum_type'].shape[0]):
                            x_i = qepro_dic['QEPro_x_axis'][i]
                            y_i = qepro_dic['QEPro_output'][i]
                            p1, p2 = da.good_bad_data(x_i, y_i, key_height = 200, data_id = f'{data_id}_{i:03d}', distance=30, height=50)
                            if (type(p1) is np.ndarray) and (type(p2) is dict):
                                _for_average[f'{data_id}_{i:03d}'] = y_i
                        
                        _for_average[f'{data_id}_mean'] = _for_average.mean(axis=1)
                        
                        x0 = x_i
                        y0 = _for_average[f'{data_id}_mean'].values
                        
                        peak, prop = da.good_bad_data(x0, y0, key_height = 200, data_id = f'{data_id}_average', distance=100, height=50)                            
                        print(f'\n** Average of {data_id} has peaks at {peak}**\n')
                        print(f'\n** start to do peak fitting by Gaussian**\n')
                        if len(peak) == 1:
                            f = da._1gauss
                            popt, _, x, y = da._1peak_fit_good_PL(x0, y0, f, peak=peak, raw_data=True)
                        if len(peak) == 2:
                            f = da._2gauss
                            popt, _, x, y = da._2peak_fit_good_PL(x0, y0, f, peak=peak, raw_data=True)
                        shift, _ = da.find_nearest(x0, x[0])
                        u.plot_peak_fit(x, y, peak-shift, f, popt, fill_between=True)
                        print(f'\n** plot fitting result complete**\n')
            print('\n*** export, identify good/bad, fitting complete ***\n')
            print('########### Events printing division ############\n')

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
    print_kafka_messages(sys.argv[1], sys.argv[2])
