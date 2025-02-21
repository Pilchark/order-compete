import threading
from order_compete.redis import OrderSystem as RedisOrderSystem
from order_compete.thread_lock import OrderSystem as LockOrderSystem
from order_compete.message_queue import OrderSystem as QueueOrderSystem

def test_redis_version():
    order_system = RedisOrderSystem()
    order_id = "order_3"
    result_1 = order_system.grab_order(order_id, "rider_1")
    result_2 = order_system.grab_order(order_id, "rider_2")
    assert result_1 is True
    assert result_2 is False

def test_thread_version():
    order_system = LockOrderSystem()
    def rider_grab(rider_id):
        result = order_system.grab_order("order123", rider_id)
        print(f"Rider {rider_id} grab result: {result}")

    # create multi threads to grab order
    threads = []
    for i in range(5):
        t = threading.Thread(target=rider_grab, args=(f"rider{i}",))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()

if __name__ == "__main__":
    # test_redis_version()
    test_thread_version()
