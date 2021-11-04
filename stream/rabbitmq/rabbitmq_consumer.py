import pika


def run(ip, port, queue, platform):
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
