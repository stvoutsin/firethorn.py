'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest


try:
    import logging
    import config as config
    import time
    from pyfirethorn import Firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = Firethorn(endpoint=config.default_endpoint + "/firethorn")
        ft.login("orinoco", "wombleden", "wombles")
        
        osa = ft.get_workspace("OSA")
        wspace = ft.new_workspace("ATLAS")
        wspace.import_schema(osa.get_schema("ATLASDR1"))
        
        # List the workspace schema.
        print (wspace.get_schemas())
        
        
        querytext = "SELECT * FROM ATLASDR1.Filter"


        # Test a Synchronous query       
        print ("Running query using Query (SYNC) class.. ")
        qry = wspace.query("Select top 2 * from ATLASDR1.Filter")
        print (qry.results().as_astropy())


        # Test an Asynchronous query       
        print ("Running query using Query (ASYNC) class.. ")
        myquery = wspace.query_async(querytext)
        myquery.run()
        while myquery.status()=="RUNNING" or myquery.status()=="READY":
            print (myquery.status())
            time.sleep(5)
        
        print (myquery.results().as_astropy())
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()