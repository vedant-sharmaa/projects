import json
import logging
import sys
import time
from typing import Any

from base import Worker
from config import config
from mrds import MyRedis
import pandas


class WcWorker(Worker):

    def run(self, **kwargs: Any) -> None:
        rds: MyRedis = kwargs['rds']

        # Write the code for the worker thread here.
        while True:
            if not rds.check_availability():
                logging.warning("Redis is not available. Waiting...")
                time.sleep(1)
                continue

            try:
                # Read
                stream = rds.rds.xreadgroup(Worker.GROUP, self.name, {
                                            config["IN"]: ">"}, count=1)
                if not stream:
                    if not rds.is_pending():
                        break
                    else:
                        self.autoclaim(rds)
                        continue

                stream_name, messages = stream[0]
                message_id, message = messages[0]
                filename = message[config["FNAME"].encode(
                    'utf-8')].decode('utf-8')

                if self.crash:
                    # DO NOT MODIFY THIS!!!
                    logging.critical(f"CRASHING!")
                    sys.exit()

                if self.slow:
                    # DO NOT MODIFY THIS!!!
                    logging.critical(f"Sleeping!")
                    time.sleep(1)

                # Process here
                word_count = self.count_words(filename)

                if word_count:
                    self.atomic_increment_and_acknowledge(rds, message_id, word_count
                                                          )
            except Exception as e:
                logging.error(f"Error while processing: {e}")
        logging.info("Exiting")

    def count_words(self, filename):
        word_count = {}
        df = pandas.read_csv(filename, lineterminator="\n")
        df["text"] = df["text"].astype(str)
        for text in df.loc[:, "text"]:
            if text == "\n":
                continue
            for word in text.split(" "):
                word_count[word] = word_count.get(word, 0) + 1
        return word_count

    def atomic_increment_and_acknowledge(self, rds, message_id, word_count):
        rds.rds.fcall(
            'atomic_increment_and_acknowledge', '4',
            config["COUNT"],
            config["IN"],
            Worker.GROUP,
            message_id,
            json.dumps(word_count)
        )

    def autoclaim(self, rds):
        while rds.is_pending():
            idle_time = 60000  # 60 seconds
            result = rds.rds.xautoclaim(
                config["IN"], Worker.GROUP, self.name, idle_time, count=1)
            
            if not result or not result[1]:
                break

            messages = result[1]
            for message_id, message in messages:
                filename = message[config["FNAME"].encode(
                    'utf-8')].decode('utf-8')
                word_count = self.count_words(filename)
                self.atomic_increment_and_acknowledge(
                    rds, message_id, word_count)
