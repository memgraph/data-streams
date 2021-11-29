from time import sleep
import json
import pika


def producer(ip, port, queue, generate, stream_delay):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(ip))
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    message = generate()
    while True:
        try:
            channel.basic_publish(
                exchange='', routing_key=queue, body=json.dumps(next(message)).encode('utf8'))
            sleep(stream_delay)
        except Exception as e:
            print(f"Error: {e}")


def consumer(ip, port, queue, platform):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=ip))
    channel = connection.channel()

    channel.queue_declare(queue=queue)

    def callback(ch, method, properties, body):
        print(platform, ": ", str(body))

    channel.basic_consume(
        queue=queue, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()
