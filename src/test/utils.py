import os, sys, unittest

class DSCTestCase(unittest.TestCase):
    'A subclass of unittest.TestCase to handle process output'
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

def load_strings(filename, group_by = 2):
    with open(filename) as f:
        idx = 0
        tmp = []
        while True:
            line = f.readline().strip()
            if not line:
                break
            if line.startswith('#'):
                continue
            tmp.append(line)
            idx += 1
            if not idx % group_by:
                yield tuple(tmp)
                tmp = []

def load_as_string(filename):
    with open(filename) as f:
        data = f.readlines()
    return ''.join(data)
