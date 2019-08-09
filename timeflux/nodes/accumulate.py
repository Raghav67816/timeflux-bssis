"""Accumulation nodes that either, stack, append or, concatenate data after a gate"""

import pandas as pd
import xarray as xr
from timeflux.core.node import Node


class AppendDataFrame(Node):
    """ Accumulates and appends data of type DataFrame after a gate

    This node should be plugged after a Gate. As long as it receives data,
    it appends them to an internal buffer. When it receives a meta with key
    'gate_status' set to 'closed', it releases the accumulated data and empty the
    buffer.

        Attributes:
        i (Port): Default data input, expects DataFrame and meta
        o (Port): Default output, provides DataFrame

   Args:
        **kwargs: key word arguments to pass to pandas.DataFrame.append method.

    """

    def __init__(self, **kwargs):

        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data = pd.DataFrame()
        self._meta = {}

    def _release(self):
        self.o.data = self._data
        self.o.meta = self._meta

    def update(self):

        gate_status = self.i.meta.get('gate_status')

        # When we have not received data, there is nothing to do
        if self.i.ready():
            return
        # At this point, we are sure that we have some data to process

        # update the meta
        self._meta.update(self.i.meta)

        # append the data
        self._data = self._data.append(self.i.data, **self._kwargs)

        # if gate is close, release the data and reset the buffer
        if gate_status == 'closed':
            self._release()
            self._reset()


class AppendDataArray(Node):
    """ Accumulates and appends data of type XArray after a gate

    This node should be plugged after a Gate. As long as it receives dataarrays,
    it appends them to a buffer list. When it receives a meta with key
    'gate_status' set to 'closed', it concatenates the list of accumulated DataArray,
    releases it and empty the buffer list.

    Attributes:
        i (Port): Default data input, expects DataArray and meta
        o (Port): Default output, provides DataArray

   Args:
        dim: Name of the dimension to concatenate along.
        **kwargs: key word arguments to pass to xarray.concat method.

    """

    def __init__(self, dim, **kwargs):

        self._dim = dim
        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data_list = []
        self._meta = {}

    def _release(self):
        self.o.data = xr.concat(self._data_list, self._dim, **self._kwargs)
        self.o.meta = self._meta

    def update(self):

        gate_status = self.i.meta.get('gate_status')

        # When we have not received data, there is nothing to do
        if self.i.ready():
            return
        # At this point, we are sure that we have some data to process

        # update the meta
        self._meta.update(self.i.meta)

        # append the data
        self._data_list.append(self.i.data)

        # if gate is close, release the data and reset the buffer
        if gate_status == 'closed':
            self._release()
            self._reset()