import datetime
import pprint
import uuid
from bluesky_kafka import RemoteDispatcher
import nslsii

def print_kafka_messages(beamline_acronym):
    print(f"Listening for Kafka messages for {beamline_acronym}")
    from databroker import Broker
    db = Broker.named("xpd")

    def print_message(name, doc):
        if name == 'stop':
            print(
                f"{datetime.datetime.now().isoformat()} document: {name}\n"
                f"contents: {pprint.pformat(doc)}\n"
            )
            print(db[doc['run_start']].table())

    kafka_config = nslsii._read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

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
