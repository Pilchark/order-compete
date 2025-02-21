import pika
import json

class OrderSystem:
    def __init__(self):
        # 建立 RabbitMQ 连接
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='localhost')
        )
        self.channel = self.connection.channel()
        
        # 声明订单队列
        self.channel.queue_declare(queue='order_queue')
        
    def publish_order(self, order_id: str):
        """
        发布订单到消息队列
        """
        self.channel.basic_publish(
            exchange='',
            routing_key='order_queue',
            body=json.dumps({'order_id': order_id}),
            properties=pika.BasicProperties(
                delivery_mode=2,  # 消息持久化
            )
        )
        
    def start_consuming(self, rider_id: str):
        """
        骑手开始监听订单
        """
        def callback(ch, method, properties, body):
            # 处理订单
            order = json.loads(body)
            print(f"Rider {rider_id} got order {order['order_id']}")
            
            # 确认消息已被处理
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        # 每次只处理一个消息
        self.channel.basic_qos(prefetch_count=1)
        
        # 开始消费消息
        self.channel.basic_consume(
            queue='order_queue',
            on_message_callback=callback
        )
        
        self.channel.start_consuming()
