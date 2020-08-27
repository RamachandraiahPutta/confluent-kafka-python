from confluent_kafka import Producer
import socket

p = Producer({
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': socket.gethostname()
})


def acked(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print("Failed to deliver message: {0}: {1}".format(
            msg.value(), err.str()))
    else:
        print('Message delivered to {0} [{1}] : Data: {2}'.format(
            msg.topic(), msg.partition(), msg.value()))


for val in range(1, 200):

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce('test', 'myvalue #{0}'.format(
        val).encode('utf-8'), callback=acked)

    # Trigger any available delivery report callbacks from previous produce() calls
    # Wait up to 1 second for events. Callbacks will be invoked during
    # this method call if the message is acknowledged.
    p.poll(0.5)

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush(30)
