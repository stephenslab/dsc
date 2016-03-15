from DSC2.dsc_file import DSCData
from pysos.utils import env
import sys

try:
    data = DSCData(sys.argv[1])
    print(data)
except Exception as e:
    env.logger.error(e)
    raise
