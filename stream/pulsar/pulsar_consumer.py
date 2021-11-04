import pulsar


def run(ip, port, topic, platform):
    client = pulsar.Client('pulsar://' + ip + ':' + port)

    consumer = client.subscribe(topic, 'my-subscription')

    while True:
        msg = consumer.receive()
        try:
            print(platform, ": ", msg.data())
            # Acknowledge successful processing of the message
            consumer.acknowledge(msg)
        except:
            # Message failed to be processed
            consumer.negative_acknowledge(msg)
    client.close()
