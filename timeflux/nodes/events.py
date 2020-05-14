"""Generate random events"""

import copy
import datetime
import json
import random
import string

import numpy as np

from timeflux.core.node import Node


class Events(Node):
    def __init__(
        self,
        rows_min=1,
        rows_max=10,
        string_min=3,
        string_max=12,
        items_min=0,
        items_max=5,
        seed=None,
    ):
        """Return random integers from value_min to value_max (inclusive)"""
        self._rows_min = rows_min
        self._rows_max = rows_max
        self._string_min = string_min
        self._string_max = string_max
        self._items_min = items_min
        self._items_max = items_max
        random.seed(seed)

    def random_string(self, length):
        return "".join(random.choice(string.ascii_letters) for m in range(length))

    def update(self):
        rows = []
        for i in range(random.randint(self._rows_min, self._rows_max)):
            row = []
            row.append(
                self.random_string(random.randint(self._string_min, self._string_max))
            )
            data = {}
            for j in range(random.randint(self._items_min, self._items_max)):
                key = self.random_string(
                    random.randint(self._string_min, self._string_max)
                )
                value = self.random_string(
                    random.randint(self._string_min, self._string_max)
                )
                data[key] = value
            row.append(json.dumps(data))
            rows.append(row)
        self.o.set(rows, names=["label", "data"])


class Periodic(Node):
    """Node that sends events at a regular interval.

    This node sends regular events after the first time the update method is
    called. If the update method is called at time `t`, and this node has a
    interval `ti` and phase `ts`, then the first event will be at `t + ts`.
    Then there will be one event at `t + ts + k ti` where `k` is 1, 2, ...

    Args:
        label (str): Event name that will be generated by this node.
        data (dict): Dictionary sent in each event.
        interval (dict): Dictionary with keyword arguments passed to
            `datetime.timedelta` to define the time interval between events.
            This can be seconds, milliseconds, microseconds, etc.
        phase (dict): Dictionary with keyword arguments passed to
            `datetime.timedelta` to define a phase for the stimulations. The
            first stimulation will happen after this time delta is observed.
            If not set, the phase will be as the interval.

    Attributes:
        o (Port): Default output, provides a pandas.DataFrame with events.

    Examples:

        The following YAML can be used to generate events every half second
        but only after 5 seconds have elapsed

        .. code-block:: yaml

           graphs:
              - nodes:
                - id: clock
                  module: timeflux.nodes.events
                  class: Periodic
                  params:
                    label: my-event-label
                    interval:
                      milliseconds: 500
                    phase:
                      seconds: 5

                - id: display
                  module: timeflux.nodes.debug
                  class: Display

                rate: 20

                edges:
                  - source: clock
                    target: display
    """

    def __init__(self, label="clock", data=None, interval=None, phase=None):
        super().__init__()

        interval = interval or {}
        phase = phase or interval
        delta_interval = datetime.timedelta(**interval)
        delta_phase = datetime.timedelta(**phase)
        if delta_interval.total_seconds() <= 0:
            raise ValueError("Periodic must have positive interval")
        if delta_phase.total_seconds() <= 0:
            raise ValueError("Periodic must have positive phase")

        self._period = np.timedelta64(delta_interval)
        self._phase = np.timedelta64(delta_phase)
        self._next_timestamp = None
        self._label = label
        self._data = data

    def update(self):
        now = np.datetime64(datetime.datetime.now())
        if self._next_timestamp is None:
            self._next_timestamp = now + self._phase
            self.logger.debug(
                "Periodic will start sending events at %s", self._next_timestamp
            )

        data = []
        times = []
        while self._next_timestamp < now:
            content = copy.deepcopy(self._data)
            data.append([self._label, content])
            times.append(self._next_timestamp)
            self._next_timestamp += self._period

        if data:
            self.logger.debug("Sending %d events", len(data))
            self.o.set(data, names=["label", "data"], timestamps=times)

    def _set_next(self, reference):
        self._next_timestamp = reference + self._period
