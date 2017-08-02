#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import json

class JupyterComposer:
    def __init__(self):
        self.text = ['{\n "cells": [']
        self.has_end = False

    def add_cell(self, content, cell = "markdown", kernel = "SoS"):
        content = [x + '\n' for x in content.split("\n")]
        self.text.append('  {\n   "cell_type": "%s",%s\n   %s\n   %s"source": %s' \
                         % (cell, '\n   "execution_count": null,' if cell == "code" else '',
                            self.get_metadata(kernel),
                            '"outputs": [],\n   'if cell == 'code' else '',
                            json.dumps(content)))
        self.text.append("  },")

    def dump(self):
        if not self.has_end:
            self.text.append(self.get_footer())
            self.has_end = True
        return '\n'.join(self.text)

    def get_footer(self):
        self.text[-1] = self.text[-1].rstrip().rstrip(',')
        return ''' ],
 "metadata": {
  "anaconda-cloud": {},
  "celltoolbar": "Tags",
  "kernelspec": {
   "display_name": "SoS",
   "language": "sos",
   "name": "sos"
  },
  "language_info": {
   "codemirror_mode": "sos",
   "file_extension": ".sos",
   "mimetype": "text/x-sos",
   "name": "sos",
   "nbconvert_exporter": "sos.jupyter.converter.SoS_Exporter",
   "pygments_lexer": "sos"
  },
  "sos": {
   "kernels": [],
   "panel": {
    "displayed": true,
    "height": 0,
    "style": "side"
   }
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}'''

    @staticmethod
    def get_metadata(kernel):
        out = '"metadata": {\n    "collapsed": true,\n    "kernel": "%s",\n    "scrolled": true,\n    "tags": []\n   },' % kernel
        return out

if __name__ == '__main__':
    jc = JupyterComposer()
    jc.add_cell("# Title")
    jc.add_cell("print(666)", cell = 'code', kernel = 'SoS')
    print(jc.dump())
