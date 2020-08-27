from confluent_kafka import Consumer, KafkaError
import time

# default.topic.config: A number of topic related configuration properties are grouped together under this high level property.
# One commonly used topic-level property is auto.offset.reset which specifies which offset to start reading from if there have been no offsets committed to a topic/partition yet.
# This defaults to latest, however you will often want this to be smallest so that old messages are not ignored when you first start reading from a topic.
c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'group.id': 'test_group',
    'enable.auto.commit': True,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['test'])

try:
    while True:
        # the poll method blocks until a Message object is ready for consumption,
        # or until the timeout period (specified in seconds) has elapsed, in which case the return value is None.
        msg = c.poll(1.0)

        if msg is None:
            continue
        elif not msg.error():
            # relevant information can be obtained via the key(), value(), timestamp(), topic(), partition() and offset() methods of the Message object.
            print('Received message: {0} Partition: {1} Offset: {2}'.format(
                msg.value().decode('utf-8'), msg.partition(), msg.offset()))
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print(
                'End of partition reached {0}/{1}'.format(msg.topic(), msg.partition()))
        else:
            print('Error occured: {0}'.format(msg.error().str()))
        time.sleep(2)

except KeyboardInterrupt:
    pass

except Exception as e:
    print("\n\n")
    print(str(e))
    print("\n\n")

finally:
    # Close down consumer to commit final offsets.
    c.close()
