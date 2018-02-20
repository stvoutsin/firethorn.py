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
    import config as config
    import time
    import firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = firethorn.Firethorn(endpoint=config.default_endpoint + "/firethorn")
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
        print ("Checking System info  as " + ft.identity())
        ft.firethorn_engine.identity.community=None
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("") 
         
        print ("Checking System info  as " + ft.identity())
        ft.firethorn_engine.identity.password=""
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("")  
         
        ft.login("orinoco", "wombleden", "wombles")
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
         
        ft.firethorn_engine.identity.community="NOT-wombles"
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("") 
            
        ft.login("orinoco", "wombleden", "wombles")
        ft.firethorn_engine.identity.password="NOT-wombleden"
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
        
        ft.login("orinoco", "NOT-wombleden", "wombles")
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("")
    
    
        ft.login("albert.augustus@example.com", "password")
        print ("Checking System info as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.system_info_check())
        print ("")
            
            
        ft = firethorn.Firethorn(endpoint=config.default_endpoint + "/firethorn")
        print ("Try creating JDBC resources as " + ft.identity())
        print (ft.firethorn_engine.identity)
                
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        jdbc_url="jdbc:jtds:sqlserver://" + config.datahost + "/ATLASDR1"
        atlas_jdbc_url = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,config.datadata, config.datacatalog, config.datatype, config.datahost, config.datauser, config.datapass)
        print (atlas_jdbc_url)
        print ("") 
    
        ft.login("orinoco", "wombleden", "wombles")
        print ("Try creating JDBC resources as " + ft.identity())
        print (ft.firethorn_engine.identity)
        print ("") 
                
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        jdbc_url="jdbc:jtds:sqlserver://" + config.datahost + "/ATLASDR1"
        atlas_jdbc_url = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,config.datadata, config.datacatalog, config.datatype, config.datahost, config.datauser, config.datapass)
        print (atlas_jdbc_url)
        print ("") 
        
    
        ft.firethorn_engine.create_temporary_identity()
        print ("Select JDBC Resource as ")
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.get_json(atlas_jdbc_url))
        print ("") 
    
    
        print ("Create ADQL Resource as ")
        print (ft.firethorn_engine.identity)
        adqlname="ATLAS ADQL resource"
        atlas_adql_url = ft.firethorn_engine.create_adql_resource(adqlname)
        print (atlas_adql_url)
        print ("") 
    
        print ("Select ADQL Resource as ")
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.get_json(atlas_adql_url))
        print ("") 
    
    
        ft.firethorn_engine.create_temporary_identity()
        print ("Select ADQL Resource as ")
        print (ft.firethorn_engine.identity)
        print (ft.firethorn_engine.get_json(atlas_adql_url))
        print ("") 
    
    
        # Create an IvoaResource to represent the GAIA TAP resource.
        tapname="GAIA TAP service"
        tapurl="http://gea.esac.esa.int/tap-server/tap"
        print ("Create an IvoaResource to represent the GAIA TAP resource as ")
        print (ft.firethorn_engine.identity)
        gaia_ivoa_resource = ft.firethorn_engine.create_ivoa_space(tapname, tapurl)
        print (gaia_ivoa_resource)
        print ("") 
            
        # Create an IvoaResource to represent the GAIA TAP resource as admin.
        ft.login("orinoco", "wombleden", "wombles")
        tapname="GAIA TAP service"
        tapurl="http://gea.esac.esa.int/tap-server/tap"
        print ("Create an IvoaResource to represent the GAIA TAP resource as ")
        print (ft.firethorn_engine.identity)
        gaia_ivoa_resource = ft.firethorn_engine.create_ivoa_space(tapname, tapurl)
        print (gaia_ivoa_resource)
        print ("") 
        
        
        # Create a new ADQL resource to act as a workspace.
        adqlname="Query workspace"
        print ("Create a new ADQL resource to act as a workspace as ")
        print (ft.firethorn_engine.identity)
        queryspace = ft.firethorn_engine.create_adql_resource(adqlname)
        print(queryspace)
        print ("")    
        
    
        # Find the AtlasDR1 schema by name.
        selector="ATLASDR1"
        print ("Find the AtlasDR1 schema by name as ")
        print (ft.firethorn_engine.identity)
        print(atlas_adql_url)
        atlas_schema = ft.firethorn_engine.select_by_name(selector, atlas_adql_url)
        print (atlas_schema)
        
        
        # Find the Gaia DR1 schema by name.
        selector="gaiadr1"
        gaia_schema = ft.firethorn_engine.select_ivoa_schema(selector, gaia_ivoa_resource)
        print (gaia_schema)
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()