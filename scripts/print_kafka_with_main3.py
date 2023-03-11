import datetime
import pprint
import uuid
from bluesky_kafka import RemoteDispatcher
from bluesky_kafka.consume import BasicConsumer
import matplotlib.pyplot as plt
import time
import databroker
from side_functions import read_qepro_by_stream, dic_to_csv_for_stream
import os
from plot_helper import plot_uvvis
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
    def print_message(name, doc):
        # print(
        #     f"{datetime.datetime.now().isoformat()} document: {name}\n"
        #     f"document keys: {list(doc.keys())}\n"
        #     # f"contents: {pprint.pformat(doc)}\n"
        # )
        if name == 'start':
            print(
                f"{datetime.datetime.now().isoformat()} documents {name}\n"
                f"document keys: {list(doc.keys())}")
                
            if 'uid' in doc.keys():
                print(f"uid: {doc['uid']}")
            if 'plan_name' in doc.keys():
                print(f"plan name: {doc['plan_name']}")
            if 'detectors' in doc.keys(): 
                print(f"detectors: {doc['detectors']}")
            if 'pumps' in doc.keys(): 
                print(f"pumps: {doc['pumps']}")
            # if 'detectors' in doc.keys(): 
            #     print(f"detectors: {doc['detectors']}")
            if 'uvvis' in doc.keys() and doc['plan_name']!='count':
                print(f"uvvis mode:\n"
                      f"           integration time: {doc['uvvis'][0]} ms\n"
                      f"           num spectra averaged: {doc['uvvis'][1]}\n"
                      f"           buffer capacity: {doc['uvvis'][2]}"
                      )
            elif 'uvvis' in doc.keys() and doc['plan_name']=='count':
                print(f"uvvis mode:\n"
                      f"           spectrum type: {doc['uvvis'][0]}\n"
                      f"           integration time: {doc['uvvis'][2]} ms\n"
                      f"           num spectra averaged: {doc['uvvis'][3]}\n"
                      f"           buffer capacity: {doc['uvvis'][4]}"
                      )                
            if 'mixer' in doc.keys():
                print(f"mixer: {doc['mixer']}")
            if 'sample_type' in doc.keys():
                print(f"sample type: {doc['sample_type']}")
            
        if name == 'stop':
            # print('Kafka test good!!')
            print(f"{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(doc)}"
            )
            # num_events = len(doc['num_events'])

            time.sleep(2)
            uid = doc['run_start']
            print(f'\n*** start to export uid: {uid} ***')
            for stream_name in ['primary', 'absorbance', 'fluorescence']:
                if stream_name in doc['num_events'].keys():
                    qepro_dic, metadata_dic = read_qepro_by_stream(uid, stream_name=stream_name, data_agent='catalog')
                    dic_to_csv_for_stream(csv_path, qepro_dic, metadata_dic, stream_name=stream_name)
                    print(f'export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(csv_path)} done!')
                    u = plot_uvvis(uid, stream_name, qepro_dic, metadata_dic)
                    u.plot_data()
            print('*** export complete ***\n')
            print('########### Events printing division ############\n')

    # plt.show()
    kafka_config = _read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"echo-{beamline_acronym}-{str(uuid.uuid4())[:8]}"

    kafka_dispatcher = RemoteDispatcher(
        topics=[f"{beamline_acronym}.bluesky.runengine.documents"],
        bootstrap_servers=",".join(kafka_config["bootstrap_servers"]),
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
    )

    kafka_dispatcher.subscribe(print_message)
    kafka_dispatcher.start()



if __name__ == "__main__":
    import sys
    print_kafka_messages(sys.argv[1], sys.argv[2])
