import datetime
import pprint
import uuid
from bluesky_kafka import RemoteDispatcher
import matplotlib.pyplot as plt

try:
    from nslsii import _read_bluesky_kafka_config_file  # nslsii <0.7.0
except (ImportError, AttributeError):
    from nslsii.kafka_utils import _read_bluesky_kafka_config_file  # nslsii >=0.7.0


def print_kafka_messages(beamline_acronym, csv_path):
    print(f"Listening for Kafka messages for {beamline_acronym}")

    from databroker import Broker
    db = Broker.named(beamline_acronym)
    plt.figure()
    def print_message(name, doc):
        # print(
        #     f"{datetime.datetime.now().isoformat()} document: {name}\n"
        #     f"contents: {pprint.pformat(doc)}\n"
        # )
        
        if name == 'start':
            print(f"{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"uid: {doc['uid']}\n"
                  f"plan name: {doc['plan_name']}\n"
                  f"detectors: {doc['detectors']}")
            if 'uvvis' in doc.keys():
                print(f"uvvis mode:\n"
                      f"           spectrum type: {doc['uvvis'][0]}\n"
                      f"           integration time: {doc['uvvis'][2]} ms\n"
                      f"           num spectra averaged: {doc['uvvis'][3]}"
                      )
            if 'mixer' in doc.keys():
                print(f"mixer: {doc['mixer']}")
            if 'sample_type' in doc.leys():
                print(f"sample type: {doc['sample_type']}")
            
        if name == 'stop':
            # print('Kafka test good!!')
            print(f"{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(doc)}\n"
            )
            uid = doc['run_start']
            print(db[uid].table())
            print(f'export directory: {csv_path}')

            x_axis_data = db[uid].table().QEPro_x_axis[1]
            output_data = db[uid].table().QEPro_output[1]
            # print(x_axis_data[:5])
            # print(output_data[:5])
            # plt.figure()
            # plt.plot(x_axis_data, output_data)
            # plt.show()
            time.sleep(2)
            qepro.export_from_scan(uid, csv_path, plot=True, data_agent='db', wait=False)
            print('Kafka printing finished!')

    plt.show()
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
