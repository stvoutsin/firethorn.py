'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest


try:
    import logging
    from models.query import Query
    from models.workspace import Workspace 
    from models import identity
    from core.firethorn_engine import FirethornEngine
    from adql_query.adql_query import AdqlQuery
    import config as config
    import time
    from pyfirethorn import Firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = Firethorn(endpoint=config.default_endpoint + "/firethorn")
        ft.login("orinoco", "wombleden", "wombles")

        
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        atlas_jdbc = ft.firethorn_engine.create_jdbc_resource("ATLAS" , config.dataurl, "ATLASDR1", jdbc_name, config.datauser, config.datapass)
        
        
        # Locate the JdbcSchema based on catalog and schema name. 
        catalog="ATLASDR1"
        schema="dbo"

        # Jdbc Schema Tests
        atlas_jdbc_schema = ft.firethorn_engine.select_jdbc_schema_by_name(atlas_jdbc.url, catalog, schema)

        # Create an empty AdqlResource to represent the local JDBC database.
        adqlname="ATLAS ADQL resource"
        atlas_adql = ft.firethorn_engine.create_adql_resource(adqlname)
        
        # Import the mapping between JDBC and ADQL tables.
        metadoc="https://raw.githubusercontent.com/stvoutsin/firethorn.py/master/firethorn/meta/ATLASDR1_TablesSchema.xml"
        atlas_adql_schema = atlas_adql.import_jdbc_schema(atlas_jdbc_schema, metadoc=metadoc)
    
    
        # Create an empty AdqlResource to represent a Query Resource
        adqlname="Query resource"
        query_resource = ft.firethorn_engine.create_adql_resource(adqlname)
        query_resource.import_adql_schema(atlas_adql_schema)

        osa = ft.get_workspace("OSA")
        wspace = ft.new_workspace("ATLAS")
        wspace.import_schema(osa.get_schema("ATLASDR1"))
        
        # List the workspace schema.
        print (wspace.get_schemas())
        
        
        querytext = "SELECT * FROM ATLASDR1.Filter"
        
        admin_query = query_resource.create_query(querytext, "COMPLETED") 
        admin_query.update(adql_query_status_next="COMPLETED")
        
        print ("Creating query using AdqlQuery.. ")
        print ("List of Running queries: ")
        print ( query_resource.select_queries())
        
        while admin_query.status()=="RUNNING" or admin_query.status()=="READY":
            print (admin_query.status())
            time.sleep(5)
            
        
        print ("Running query using Query (SYNC) class.. ")
        qry = wspace.query("Select top 2 * from ATLASDR1.Filter")
        print (qry.results().as_astropy())


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