"""
Import dependencies

changes: from data_manager import DataManager
instead of zmq
"""
import time
import pandas
from data_manager import DataManager
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.io import Port
import timeflux.core.message


class Broker(Node):

    """Must run in its own graph"""
    def __init__(self):
        """
        from original zmq.py it can be said that the frontend is a SUBSCRIBER
        and the backend is the PUBLISHER
        """
        try:
            self._frontend = DataManager("$subscriber profile")
            self._backend = DataManager("$publisher profile")

        except Exception as error:
            self.logger.error(error)

    def update(self):
        """Start non-blocking threads"""
        self._frontend.listen()
        

class Pub(Node):
    def __init__(self, topic, serializer="pickle", wait=0):
        #Make another publisher
        self._topic = topic.encode("utf-8")
        self._serializer = getattr(timeflux.core.message, serializer + "_serialize")
        try:
            self.publisher = DataManager("$Another publisher profile")
        except Exception as error:
            self.logger.error(error)

        time.sleep(wait)

    def update(self):
        for name, suffix, port in self.iterate("i*"):
            if port.ready() or port.meta:
                if not suffix:
                    topic = self._topic
                else:
                    topic = self._topic + suffix.encode("utf-8")
                try:
                    if not port.ready():
                        port.data = None  
                    data = self._serializer([topic, port.data, port.meta])
                    self._publisher.set_data(data)
                    self._publisher.publish(0)  
                except Exception as e:
                    self.logger.error(e)
    
