# Example modified from 
# 
# https://github.com/confluentinc/confluent-kafka-python/blob/d9a91a3d74c6447d53ba1c5733b0f99c2db2e259/examples/confluent_cloud.py
#
#
import boto3
import json
import uuid
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
client = boto3.client('secretsmanager')
secret_response = response = client.get_secret_value(
    SecretId='<Secret ARN>',
)

# grab the username and password from the secret
secret_string = json.loads(secret_response['SecretString'])
username = secret_string['username'] 
password = secret_string['password']
# print(username)
# print(password)
def error_cb(err):
    """ The error callback is used for generic client errors. These
        errors are generally to be considered informational as the client will
        automatically try to recover from all errors, and no extra action
        is typically required by the application.
        For this example however, we terminate the application if the client
        is unable to connect to any broker (_ALL_BROKERS_DOWN) and on
        authentication errors (_AUTHENTICATION). """
    print("Client error: {}".format(err))
    if err.code() == KafkaError._ALL_BROKERS_DOWN or \
       err.code() == KafkaError._AUTHENTICATION:
        # Any exception raised from this callback will be re-raised from the
        # triggering flush() or poll() call.
        raise KafkaException(err)
# Create producer
p = Producer({
    'bootstrap.servers': '<MSK cluster server list>',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'security.protocol': 'SASL_SSL',
    'sasl.username': username,
    'sasl.password': password,
    'error_cb': error_cb,
})
def acked(err, msg):
    """Delivery report callback called (from flush()) on successful or failed delivery of the message."""
    if err is not None:
        print('Failed to deliver message: {}'.format(err.str()))
    else:
        print('Produced to: {} [{}] @ {}'.format(msg.topic(), msg.partition(), msg.offset()))
for n in range(0, 10):
    # Produce message: this is an asynchronous operation.
    # Upon successful or permanently failed delivery to the broker the
    # callback will be called to propagate the produce result.
    # The delivery callback is triggered from poll() or flush().
    # For long running
    # produce loops it is recommended to call poll() to serve these
    # delivery report callbacks.
    #
    # Enter the topic below
    #
    p.produce('<topic name>', value='python test value nr {}'.format(n),
              callback=acked)
    # Trigger delivery report callbacks from previous produce calls.
    p.poll(0)
# flush() is typically called when the producer is done sending messages to wait
# for outstanding messages to be transmitted to the broker and delivery report
# callbacks to get called. For continous producing you should call p.poll(0)
# after each produce() call to trigger delivery report callbacks.
p.flush(10)
# Create consumer
c = Consumer({
    'bootstrap.servers': '<MSK cluster server list>',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'security.protocol': 'SASL_SSL',
    'sasl.username': username,
    'sasl.password': password,
    'group.id': str(uuid.uuid1()),  # this will create a new consumer group on each invocation.
    'auto.offset.reset': 'earliest',
    'error_cb': error_cb,
})
#
# Enter the topic below
#
c.subscribe(['test'])
try:
    while True:
        msg = c.poll(0.1)  # Wait for message or event/error
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to `session.timeout.ms` for
            #   the group to rebalance and start consuming.
            continue
        if msg.error():
            # Errors are typically temporary, print error and continue.
            print('Consumer error: {}'.format(msg.error()))
            continue
        print('Consumed: {}'.format(msg.value()))
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    c.close()