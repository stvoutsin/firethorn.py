import logging
import sys, os
testdir = os.path.dirname(__file__)
sys.path.insert(0, os.path.dirname(__file__))

try:
    import query_engine
    import firethorn_engine
    import auth_engine
except Exception as e:
    print ("Error during py imports..(py.py): " + str(e))
    logging.exception(e)
