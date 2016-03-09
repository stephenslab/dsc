import os, sys, unittest

class DSCTestCase(unittest.TestCase):
    'A subclass of unittest.TestCase to handle process output'
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)

def load_strings(filename, group_by = 2):
    yield tuple(['' for i in range(group_by)])
