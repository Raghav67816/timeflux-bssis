"""
Import dependencies

changes: from data_manager import DataManager
instead of zmq
"""
import time
import pandas
import timeflux.core.message
from timeflux.core.io import Port
from data_manager import DataManager
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt


# class Broker(Node):
# 
    # """Must run in its own graph"""
    # def __init__(self):
        # """
        # from original zmq.py it can be said that the frontend is a SUBSCRIBER
        # and the backend is the PUBLISHER
        # """
        # try:
            # self._frontend = DataManager("$subscriber profile")
            # self._backend = DataManager("$publisher profile")
# 
        # except Exception as error:
            # self.logger.error(error)
# 
    # def update(self):
        # """Start non-blocking threads"""
        # self._frontend.listen()


class BrokerMonitored(Node):

    """
    Run a monitored pub/sub proxy.
    Will shut itself down after [timeout] seconds if no data is received.
    Useful for unit testing and replays.
    """

    _last_event = time.time()

    def __init__(
        self,
        data_manager_client: DataManager,
        address_in="tcp://127.0.0.1:5559",
        address_out="tcp://127.0.0.1:5560",
        timeout=5,
    ):
        self._timeout = timeout

        try:
            # Capture
            address_monitor = "inproc://monitor"
            context = zmq.Context.instance()
            self._monitor = context.socket(zmq.PULL)
            self._monitor.bind(address_monitor)
            self._data_manager_client = data_manager_client

            # Proxy
            proxy = ThreadProxy(zmq.XSUB, zmq.XPUB, zmq.PUSH)
            proxy.bind_in(address_in)
            proxy.bind_out(address_out)
            proxy.connect_mon(address_monitor)
            # proxy.setsockopt_mon(zmq.CONFLATE, True) # Do not clutter the network
            proxy.start()

        except zmq.ZMQError as error:
            self.logger.error(error)



class BrokerLVC(Node):
    """A monitored pub/sub broker with last value caching."""

    def __init__(
        self,
        address_in="tcp://127.0.0.1:5559",
        address_out="tcp://127.0.0.1:5560",
        timeout=1000,
    ):
        self._timeout = timeout
        try:
            context = zmq.Context.instance()
            self._frontend = context.socket(zmq.SUB)
            self._frontend.setsockopt(zmq.SUBSCRIBE, b"")
            self._frontend.bind(address_in)
            self._backend = context.socket(zmq.XPUB)
            self._backend.setsockopt(zmq.XPUB_VERBOSE, True)
            self._backend.bind(address_out)
            self._poller = zmq.Poller()
            self._poller.register(self._frontend, zmq.POLLIN)
            self._poller.register(self._backend, zmq.POLLIN)
        except zmq.ZMQError as error:
            self.logger.error(error)

    def update(self):
        """Main poll loop."""
        cache = {}
        while True:
            events = dict(self._poller.poll(self._timeout))
            # Any new topic data we cache and then forward
            if self._frontend in events:
                message = self._frontend.recv_multipart()
                topic, current = message
                cache[topic] = current
                self._backend.send_multipart(message)
            # handle subscriptions
            # When we get a new subscription we pull data from the cache:
            if self._backend in events:
                event = self._backend.recv()
                # print(event)
                # Event is one byte 0=unsub or 1=sub, followed by topic
                # if event[0] == b'\x01':
                if event[0] == 1:
                    topic = event[1:]
                    if topic in cache:
                        self.logger.debug(
                            "Sending cached topic %s", topic.decode("utf-8")
                        )
                        self._backend.send_multipart([topic, cache[topic]])


class Broker(Node):

    """Must run in its own graph."""

    def __init__(
        self, address_in="tcp://127.0.0.1:5559", address_out="tcp://127.0.0.1:5560"
    ):
        """
        Initialize frontend and backend.
        If used on a LAN, bind to tcp://*:5559 and tcp://*:5560 instead of localhost.
        """
        try:
            context = zmq.Context.instance()
            self._frontend = context.socket(zmq.XSUB)
            self._frontend.bind(address_in)
            self._backend = context.socket(zmq.XPUB)
            self._backend.bind(address_out)
        except zmq.ZMQError as e:
            self.logger.error(e)

    def update(self):
        """Start a blocking proxy."""
        zmq.proxy(self._frontend, self._backend)



class Pub(Node):
    def __init__(
        self, topic, serializer="pickle", wait=0
    ):

    # Make another publisher
    self.publisher = None
    self._topic = topic.encode('utf-8')
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
                    topic = self._topic + suffix.encode('utf-8')
                try:
                    if not port.ready():
                        port.data = None
                    self.publisher.set_data(serialize(
                        [topic, port.data, port.meta], self._serializer
                    ))

                except Exception as error:
                    self.logger.error(error)

class Sub(Node):
    def __init__(
        self, topics=[""], deserializer="pickle"
    ):
        try:
            self.subscriber = DataManager("$A SUBSCRIBER PROFILE", self.deserialize)
            for topic in topics:
                pass
                if topic:
                    if not topic.isidentifier():
                        raise ValueError(f"Invalid topic name: {topic}")
            self._deserializer = getattr(
            timeflux.core.message, deserializer + "_deserialise"
         )

        except Exception as error:
            self.logger.error(error)


    def update(self):
        self._chunks = {}
        try:
            while True:
                [topic, data, meta] = self.subscriber.listen()
                if not topic in self._chunks:
                    self._chunks[topic] = {"data": [], "meta": {}}
                self._append_data(topic, data)
                self._append_meta(topic, meta)

        except Exception as error:
            pass
        self._update_ports()

     def _append_data(self, topic, data):
        if data is not None:
            self._chunks[topic]["data"].append(data)

    def _append_meta(self, topic, meta):
        if meta:
            self._chunks[topic]["meta"].update(meta)

    def _update_ports(self):
        for topic in self._chunks.keys():
            if len(self._chunks[topic]["data"]) == 0:
                data = None
            elif len(self._chunks[topic]["data"]) == 1:
                data = self._chunks[topic]["data"][0]
            else:
                data = pandas.concat(self._chunks[topic]["data"])
            meta = self._chunks[topic]["meta"]
            self._update_port(topic, data, meta)

    def _update_port(self, topic, data, meta):
        getattr(self, "o_" + topic).data = data
        getattr(self, "o_" + topic).meta = meta


    def deserialize(self, obj: bytes, load_func: Callable[[bytes], Any]):
        return load_func(obj)
        
    
