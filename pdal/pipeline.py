
from pdal import libpdalpython
import numpy as np
import json

class Pipeline(object):
    """A PDAL pipeline object, defined by JSON. See http://www.pdal.io/pipeline.html for more
    information on how to define one"""

    def __init__(self, json=None, arrays=None):

        self._p = None

        if isinstance(json, str):
            if arrays:
                self._p = libpdalpython.PyPipeline(json, arrays)
            else:
                self._p = libpdalpython.PyPipeline(json)
            return
        
        if isinstance(json, dict):
            self.p = json
        elif json is None:
            self.p = {'pipeline':[]}

    def add_file(self, filename, **kwargs):
        kwargs['filename'] = filename
        self.p['pipeline'].append(kwargs)

    def add_filter(self, **kwargs):
        self.p['pipeline'].append(kwargs)

    def add_crop(self, bounds=None, polygon=None, **kwargs):

        if bounds is None and polygon is None:
            print('add_clip requires one of bounds or polygon')
            return
        crop_item = kwargs
        crop_item['type'] = 'filters.crop'
        # add bounds
        if bounds is not None:
            l,b,r,t = bounds
            crop_item['bounds'] = f'([{l},{r}],[{b},{t}])'
        if polygon is not None:
            crop_item['polygon'] = polygon

        self.add_filter(**crop_item)

    def create(self):
        """
        Method to convert Python Dictionary Pipeline to json and build
        PDAL Pipeline object.
        """
        json_str = json.dumps(self.p)
        if self.arrays:
            self._p = libpdalpython.PyPipeline(json_str)
        else:
            self._p = libpdalpython.PyPipeline(json_str, self.arrays)

    @property
    def metadata(self):
        """
        PDAL pipeline metadata
        """
        if self._p is None:
            return None
        else:
            return self._p.metadata

    @property
    def schema(self):
        if self._p is None:
            return None
        else:
            return self._p.schema

    @property
    def pipeline(self):
        if self._p is None:
            return None
        else:
            return self._p.pipeline

    @property
    def loglevel(self):
        if self._p is None:
            return None
        else:
            return self._p.loglevel
    
    @loglevel.setter
    def loglevel(self, v):
        if self._p is not None:
            self._p.loglevel = v

    @property
    def log(self):
        if self._p is None:
            return None
        else:
            return self._p.log

    @property
    def arrays(self):
        if self._p is None:
            return None
        else:
            return self._p.arrays

    def execute(self):
        return self._p.execute()

    def validate(self):
        return self._p.validate()

