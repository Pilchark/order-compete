import redis
import time

class OrderSystem:
    def __init__(self):
        # init redis
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        
    def grab_order(self, order_id: str, rider_id: str) -> bool:
        """
        compete order method
        :param order_id: order id
        :param rider_id: rider id
        :return: grab order result
        """
        # 使用 Redis 的 setnx 命令实现分布式锁
        lock_key = f"order_lock:{order_id}"
        
        # 尝试获取锁，设置过期时间为 5 秒
        if self.redis_client.setnx(lock_key, rider_id):
            try:
                # 设置锁的过期时间
                self.redis_client.expire(lock_key, 5)
                
                # 检查订单是否已被抢
                order_status = self.redis_client.get(f"order:{order_id}:status")
                if order_status and order_status.decode() == "taken":
                    return False
                
                # 将订单分配给骑手
                self.redis_client.set(f"order:{order_id}:rider", rider_id)
                self.redis_client.set(f"order:{order_id}:status", "taken")
                print(f"Rider {rider_id} grab order {order_id}")
                
                return True
            finally:
                # 释放锁
                self.redis_client.delete(lock_key)
        return False
