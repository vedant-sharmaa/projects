from __future__ import annotations

import logging
import time
from typing import Optional, Final

from redis.client import Redis

from base import Worker
from config import config

import subprocess

class MyRedis:
    def __init__(self):
        self.rds: Final = Redis(host='localhost', port=6379, password=None,
                                db=0, decode_responses=False)
        self.rds.flushall()
        self.rds.xgroup_create(
            config["IN"], Worker.GROUP, id="0", mkstream=True)
        
    def add_file(self, fname: str):
        self.rds.xadd(config["IN"], {config["FNAME"]: fname})

    def top(self, n: int) -> list[tuple[bytes, float]]:
        return self.rds.zrevrangebyscore(config["COUNT"], '+inf', '-inf', 0, n,
                                         withscores=True)

    def is_pending(self) -> bool:
        try:
            pending = self.rds.xpending_range(
                config["IN"], Worker.GROUP, "-", "+", 1
            )
            pending_messages = len(pending)
            logging.info(f"Pending messages: {pending_messages}")
            return pending_messages > 0
        except Exception as e:
            logging.error(f"Error checking pending messages: {e}")
            return False

    def restart(self, downtime: int):
        logging.info("Shutting down Redis container...")
        subprocess.run(["docker", "stop", "redis"])

        logging.info(f"Redis is down. Waiting for {downtime} seconds...")
        time.sleep(downtime)

        logging.info("Starting Redis container...")
        subprocess.run([
            "docker", "run", "-d", "-p", "6379:6379", "-v", "/home/vedant/redis:/data", 
            "--name", "redis", "--rm", "redis:7.4"
        ]) 

        max_retries = 10
        for i in range(max_retries):
            try:
                self.rds = Redis(host='localhost', port=6379, password=None,
                                 db=0, decode_responses=False)
                self.rds.ping()
                logging.info("Successfully reconnected to Redis")
                break
            except:
                if i < max_retries - 1:
                    logging.info(f"Failed to connect. Retrying in 1 second... (Attempt {i+1}/{max_retries})")
                    time.sleep(1)
                else:
                    logging.error("Failed to reconnect to Redis after multiple attempts")
                    raise
        
    def check_availability(self):
        try:
            self.rds.ping()
            return True
        except:
            return False
