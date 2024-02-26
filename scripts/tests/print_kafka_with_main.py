import datetime
import pprint
import uuid
from bluesky_kafka import RemoteDispatcher
from bluesky_queueserver.manager.comms import zmq_single_request

try:
    from nslsii import _read_bluesky_kafka_config_file  # nslsii <0.7.0
except (ImportError, AttributeError):
    from nslsii.kafka_utils import _read_bluesky_kafka_config_file  # nslsii >=0.7.0


def print_kafka_messages(beamline_acronym):
    print(f"Listening for Kafka messages for {beamline_acronym}")

    from databroker import Broker
    db = Broker.named(beamline_acronym)

    def print_message(name, doc):
        print(
            f"{datetime.datetime.now().isoformat()} document: {name}\n"
            f"contents: {pprint.pformat(doc)}\n"
        )
        if name == 'stop':
            print(db[doc['run_start']].table())

            # zmq_single_request(method='queue_item_add', params={'item':{"name":"insitu_test", "args": [1 ,1]
            # , "kwargs": {"sample": "quinine_qserver", "csv_path": "/home/xf28id2/Documents/ChengHung/20230403_qserver_collection", "data_agent":"tiled"}, "item_type":"plan"}, 'user_group':'primary', 'user':'chlin'})

            # zmq_single_request(method='queue_start')
            
    
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
    print_kafka_messages(sys.argv[1])
