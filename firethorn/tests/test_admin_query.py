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

        
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        atlas_jdbc = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,firethorn.config.datadata, firethorn.config.datacatalog, firethorn.config.datatype, firethorn.config.datahost, firethorn.config.datauser, firethorn.config.datapass)
        
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

        querytext = "SELECT * FROM ATLASDR1.Filter"

        print ("Creating query using AdqlQuery.. ")
        admin_query = query_resource.create_query(querytext) 
        print (admin_query)
        
        
        print ("List of Running queries: ")
        print ( query_resource.select_queries())

        admin_query = admin_query.update(adql_query_status_next="COMPLETED") 
        
        while admin_query.isRunning():
            print (admin_query.status())
            time.sleep(5)

        print (admin_query)

        print (admin_query.results())
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
