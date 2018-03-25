'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest

try:
    import logging
    import firethorn
    import urllib
    import json
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testSetupTap(self):
        
        with open('../data/osa-tap.json') as json_data:
            json_obect = json.load(json_data)
            name = json_obect.get("name")
            databases = json_obect.get("Databases")
            jdbc_resource = json_obect.get("JdbcResource")
            metadata = json_obect.get("Metadata")

        
        ft = firethorn.Firethorn(endpoint="http://gworewia.metagrid.xyz/firethorn")
        ft.login(firethorn.config.adminuser, firethorn.config.adminpass, firethorn.config.admingroup)
        
        
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name= name + " JDBC resource"   
        tap_jdbc = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,jdbc_resource["datadata"], jdbc_resource["datacatalog"], jdbc_resource["datatype"], jdbc_resource["datahost"], jdbc_resource["datauser"], jdbc_resource["datapass"])
        
        print (tap_jdbc)
        
        # Create an empty AdqlResource to represent the local JDBC database.
        tap_name= name + " ADQL resource"
        tap_adql = ft.firethorn_engine.create_adql_resource(tap_name)
        

        for database in databases:
            jdbc_schema = tap_jdbc.select_schema_by_name(
                database.get("name"),
                "dbo"
                )
            if (jdbc_schema!=None):
                metadoc="https://raw.githubusercontent.com/wfau/metadata/master/metadocs/" + database.get("name") + "_TablesSchema.xml"
                adql_schema = tap_adql.import_jdbc_schema(
                    jdbc_schema,
                    database.get("name"),
                    metadoc=metadoc
                    )
                print (adql_schema)
        
        print (tap_adql)
            
        tap_schema_user = metadata["user"]
        tap_schema_pass = metadata["pass"]
        tap_schema_url = metadata["url"]
        tap_schema_driver =  "net.sourceforge.jtds.jdbc.Driver"
        tap_schema_db = metadata["database"]
        
        data = urllib.parse.urlencode({"url": tap_schema_url, 
                                       "user" : tap_schema_user, 
                                       "pass" : tap_schema_pass, 
                                       "driver" : tap_schema_driver, 
                                       "catalog" : tap_schema_db }).encode("utf-8")

        # Generate TAP_SCHEMA
        req = urllib.request.Request( ft.endpoint + "/tap/"+ tap_adql.ident() + "/generateTapSchema", headers=tap_adql.account.get_identity_as_headers())
        response = urllib.request.urlopen(req,data)
        response.close()

        print (ft.endpoint + "/tap/"+ tap_adql.ident())
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
