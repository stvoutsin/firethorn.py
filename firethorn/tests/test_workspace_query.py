'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest


try:
    import logging
    import time
    import firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = firethorn.Firethorn(endpoint=firethorn.config.default_endpoint)
        ft.login(firethorn.config.adminuser, firethorn.config.adminpass, firethorn.config.admingroup)

        # Get list of Public (WFAU) Resources
        ft.get_public_resource_names() 
        # Select a Resource by name
        osa_resource = ft.get_public_resource_by_name('OSA')
        # Get list of Schemas for OSA resource
        osa_resource.get_schema_names()
        atlasdr1 = osa_resource.get_schema_by_name("ATLASDR1")
        # Get list of Tables for ATLASDR1 Schema
        atlasdr1.get_table_names()
        atlasSource = atlasdr1.get_table_by_name("atlasSource")
        # Get list of Columns for atlasSource Table
        atlasSource.get_column_names()
        ra = atlasSource.get_column_by_name("ra")
        # Create new workspace
        my_workspace = ft.new_workspace("mySpace")
        # Import a Schema from the OSA workspace
        my_workspace.add_schema(
            osa_resource.get_schema_by_name(
                "ATLASDR1"
            )
        )
        
        # List the workspace schema.
        print (my_workspace.get_schemas())
        # List the workspace schema.
        my_workspace.get_schema_names()

        # Run an Asynchronous query
        async_query = my_workspace.query("SELECT TOP * FROM ATLASDR1.Filter", "ASYNC")
        # Test an Asynchronous query       
        print (async_query.error())
        async_query.update("SELECT * FROM ATLASDR1.Filter")
        
        async_query.run()
        while async_query.isRunning():
            print (async_query.status())
            time.sleep(5)
        
        print (async_query.results().as_astropy())
                
                
        
        # Run a Synchronous query
        query = my_workspace.query("SELECT TOP 10 ra,dec FROM ATLASDR1.atlasSource")
        # Get results table
        table = query.results()
        # Get results table row count
        table.rowcount()
        # Get results table as astropy table
        table.as_astropy()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
