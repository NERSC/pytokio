#!/usr/bin/env python

import json

class NerscIsdct(dict):
    def __init__(self, input_file):
        super(NerscIsdct,self).__init__(self)
        self.input_file = input_file
        self.load()

    def __repr__(self):
        return json.dumps(self.values())

    def load(self):
        """
        Infer the type of input we're receiving, dispatch the correct loading
        function, and populate keys/values
        """
        raise NotImplementedError
 
    def save_cache(self, output_file=None):
        """
        Save the dictionary in a json file
        """
        raise NotImplementedError

    def _save_cache(self, output):
        output.write(str(self))
