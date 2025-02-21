import threading
from typing import Dict

class OrderSystem:
    def __init__(self):
        self.orders: Dict[str, str] = {}  # 订单状态存储
        self.lock = threading.Lock()      # 线程锁
        
    def grab_order(self, order_id: str, rider_id: str) -> bool:
        """
        骑手抢单方法
        :param order_id: 订单ID
        :param rider_id: 骑手ID
        :return: 是否抢单成功
        """
        with self.lock:
            # 检查订单是否已被抢
            if order_id in self.orders:
                return False
            
            # 将订单分配给骑手
            self.orders[order_id] = rider_id
            print(f"Rider {rider_id} grab order {order_id}")
            return True
