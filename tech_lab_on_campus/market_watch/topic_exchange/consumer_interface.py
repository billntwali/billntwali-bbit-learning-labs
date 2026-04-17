# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import os
import pika


class mqConsumerInterface:
    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic"
        )

    def createQueue(self, queueName: str) -> None:
        self.channel.queue_declare(queue=queueName)
        self.channel.basic_consume(
            queueName, self.on_message_callback, auto_ack=False
        )

    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        self.channel.queue_bind(
            queue=queueName,
            exchange=self.exchange_name,
            routing_key=topic,
        )

    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        channel.basic_ack(method_frame.delivery_tag, False)
        print(body.decode("utf-8"))

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        try:
            self.channel.close()
        except Exception:
            pass
        try:
            self.connection.close()
        except Exception:
            pass
