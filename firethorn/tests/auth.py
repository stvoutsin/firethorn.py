'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest

try:
    import logging
    import config as config
    import firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = firethorn.Firethorn(endpoint=config.default_endpoint + "/firethorn")
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
        print ("Checking System info  as " + str(ft.identity()))
        ft.firethorn_engine.auth_engine.community=None
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("") 
         
        print ("Checking System info  as " + str(ft.identity()))
        ft.firethorn_engine.auth_engine.password=""
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("")  
         
        ft.login("orinoco", "wombleden", "wombles")
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
         
        ft.firethorn_engine.auth_engine.community="NOT-wombles"
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("") 
            
        ft.login("orinoco", "wombleden", "wombles")
        ft.firethorn_engine.auth_engine.password="NOT-wombleden"
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("") 
        
        
        ft.login("orinoco", "NOT-wombleden", "wombles")
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("")
    
    
        ft.login("albert.augustus@example.com", "password")
        print ("Checking System info as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.system_info_check())
        print ("")
            
            
        ft = firethorn.Firethorn(endpoint=config.default_endpoint + "/firethorn")
        print ("Try creating JDBC resources as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
                
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        jdbc_url="jdbc:jtds:sqlserver://" + config.datahost + "/ATLASDR1"
        atlas_jdbc_url = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,config.datadata, config.datacatalog, config.datatype, config.datahost, config.datauser, config.datapass)
        print (atlas_jdbc_url)
        print ("") 
    
        ft.login("orinoco", "wombleden", "wombles")
        print ("Try creating JDBC resources as " + str(ft.identity()))
        print (ft.firethorn_engine.identity())
        print ("") 
                
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        jdbc_url="jdbc:jtds:sqlserver://" + config.datahost + "/ATLASDR1"
        atlas_jdbc_url = ft.firethorn_engine.create_jdbc_resource(jdbc_name ,config.datadata, config.datacatalog, config.datatype, config.datahost, config.datauser, config.datapass)
        print (atlas_jdbc_url)
        print ("") 
        
    
        ft.firethorn_engine.create_temporary_auth()
        print ("Select JDBC Resource as ")
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.get_json(atlas_jdbc_url.url))
        print ("") 
    
    
        print ("Create ADQL Resource as ")
        print (ft.firethorn_engine.identity())
        adqlname="ATLAS ADQL resource"
        atlas_adql_url = ft.firethorn_engine.create_adql_resource(adqlname)
        print (atlas_adql_url)
        print ("") 
    
        print ("Select ADQL Resource as ")
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.get_json(atlas_adql_url.url))
        print ("") 
    
    
        ft.firethorn_engine.create_temporary_auth()
        print ("Select ADQL Resource as ")
        print (ft.firethorn_engine.identity())
        print (ft.firethorn_engine.get_json(atlas_adql_url.url))
        print ("") 
    
        pass

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()