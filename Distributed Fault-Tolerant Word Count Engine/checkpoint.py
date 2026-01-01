import logging
import threading
import time
from mrds import MyRedis

def create_checkpoints(rds: MyRedis, interval: int):
    def checkpoint():
        while True:
            try:
                logging.info("Creating checkpoint")
                rds.rds.bgsave()
            except Exception as e:
                logging.error(f"Error creating checkpoint: {e}")
            time.sleep(interval)
    
    checkpoint_thread = threading.Thread(target=checkpoint, daemon=True)
    checkpoint_thread.start()

