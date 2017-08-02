#!/usr/bin/env python
__author__ = "Gao Wang"
__copyright__ = "Copyright 2016, Stephens lab"
__email__ = "gaow@uchicago.edu"
__license__ = "MIT"
import json, os

HOME_DOC = '''
This page displays contents of DSC database `NAME`. There are two types of tables in this database:

* **pipeline** tables: *add description*
* **module** tables: *add description*

[DSC script](#DSC-script) that generated this database can be found at the end of this page.
'''

def get_database_summary(db, title = "Database Summary"):
    import pickle
    jc = JupyterComposer()
    jc.add("# {}\n{}".format(title, HOME_DOC.replace("NAME", os.path.basename(db))))
    jc.add('''
import pickle
data = pickle.load(open("{}", 'rb'))
    '''.format(os.path.expanduser(db)), cell = "code")
    data = pickle.load(open(os.path.expanduser(db), 'rb'))
    jc.add("<hr>")
    jc.add("## Pipelines")
    for key in data:
        if key.startswith('pipeline_'):
            jc.add("<hr>")
            jc.add("### pipeline `{}`".format(key[9:]))
            jc.add("%preview -n data['{}']".format(key), cell = "code", out = True)
    jc.add("<hr>")
    jc.add("## Modules")
    for key in data:
        if not key.startswith("pipeline_") and not key.startswith('.'):
            jc.add("<hr>")
            jc.add("### module `{}`".format(key))
            jc.add("%preview -n data['{}']".format(key), cell = "code", out = True)
    jc.add("<hr>")
    jc.add("## DSC script")
    jc.add("print(eval(data['.dscsrc']))", cell = "code", out = True)
    return jc.dump()

class JupyterComposer:
    def __init__(self):
        self.text = ['{\n "cells": [']
        self.has_end = False

    def add(self, content, cell = "markdown", kernel = "SoS", out = False):
        content = [x + '\n' for x in content.strip().split("\n")]
        self.text.append('  {\n   "cell_type": "%s",%s\n   %s\n   %s"source": %s' \
                         % (cell, '\n   "execution_count": null,' if cell == "code" else '',
                            self.get_metadata(cell, kernel, out),
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
    def get_metadata(cell, kernel, out):
        out = '"metadata": {\n    "collapsed": true,\n    "kernel": "%s",\n    "scrolled": true,\n    "tags": [%s]\n   },' % (kernel, '"report_output"' if cell == 'code' and out else '')
        return out

if __name__ == '__main__':
    # jc = JupyterComposer()
    # jc.add("# Title")
    # jc.add("print(666)", cell = 'code', kernel = 'SoS')
    # jc.add("print(999)", cell = 'code', kernel = 'SoS', out = True)
    # print(jc.dump())
    import sys
    print(get_database_summary(sys.argv[1]))
