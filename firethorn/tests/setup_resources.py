'''
Created on Feb 7, 2018

@author: stelios
'''
import unittest

try:
    import logging
    import config as config
    from pyfirethorn import Firethorn
except Exception as e:
    logging.exception(e)

class Test(unittest.TestCase):


    def testAuth(self):
        ft = Firethorn(endpoint=config.default_endpoint + "/firethorn")
        ft.login("orinoco", "wombleden", "wombles")

        
        #  Create a JdbcResource to represent the local JDBC database.
        jdbc_name="ATLAS JDBC resource"
        atlas_jdbc = ft.firethorn_engine.create_jdbc_resource(jdbc_name , config.datadata, config.datacatalog, config.datatype, config.datahost, config.datauser, config.datapass)
        print ("atlas_jdbc: " +  str(atlas_jdbc))
        print ("Ident: " + atlas_jdbc.ident())
        print ("Name: " + atlas_jdbc.name())
        print ("Owner: " + atlas_jdbc.owner())
        print ("URL: " + atlas_jdbc.url)   
        
        
        print ("select_schemas() : ")
        print (atlas_jdbc.select_schemas()) 
        print ("select_schema_by_ident(): ")
        print (atlas_jdbc.select_schema_by_ident(atlas_jdbc.select_schemas()[0].get("self"))) 
        print ("select_schema_by_name(): ")
        print (atlas_jdbc.select_schema_by_name("ATLASDR1", "dbo")) 
        print ("create_schema(): ") 
        print (atlas_jdbc.create_schema("dbo", "mySchema") ) ## ???? Not implemented yet
        
     
        
        # Locate the JdbcSchema based on catalog and schema name. 
        catalog="ATLASDR1"
        schema="dbo"
        
        print ("select_jdbc_resource_by_name(): ")
        print (ft.firethorn_engine.select_jdbc_resource_by_name(catalog)) ## ???  ## ???? Not implemented yet
        print ("select_jdbc_resource_by_ident(): ")
        print ( ft.firethorn_engine.select_jdbc_resource_by_ident(atlas_jdbc.url))
        
        
        # Jdbc Schema Tests
        atlas_jdbc_schema = ft.firethorn_engine.select_jdbc_schema_by_name(atlas_jdbc.url, catalog, schema)
        filter_jdbc_table = atlas_jdbc_schema.select_table_by_ident(atlas_jdbc_schema.select_tables()[0].get("self"))

        print ("atlas_jdbc_schema: ")
        print (atlas_jdbc_schema)
        print ("resource(): ")
        print (atlas_jdbc_schema.resource())
        print ("catalog_name(): " + atlas_jdbc_schema.catalog_name())
        print ("select_tables(): ")
        print (atlas_jdbc_schema.select_tables())
        print ("select_table_by_ident(): ")
        print (atlas_jdbc_schema.select_table_by_ident(atlas_jdbc_schema.select_tables()[0].get("self")))
        print ("select_table_by_name(): ") 
        print (atlas_jdbc_schema.select_table_by_name(filter_jdbc_table.name)) ## ???? ## ???? Not implemented yet
        print ("create_table(): ") 
        print (atlas_jdbc_schema.create_table("myTable")) ## ???? ## ???? Not implemented yet
    
    
        # Jdbc Table Tests
        print ("name(): " + filter_jdbc_table.name())
        print ("ident(): " + filter_jdbc_table.ident())
        print ("resource(): ") 
        print (filter_jdbc_table.resource())
        print ("schema(): ") 
        print (filter_jdbc_table.schema())
        print ("select_columns(): ") 
        print (filter_jdbc_table.select_columns())
        print ("select_column_by_ident(): ")
        filterID_jdbc_column = filter_jdbc_table.select_column_by_ident(filter_jdbc_table.select_columns()[0].get("self"))
        print (filterID_jdbc_column)
        print ("select_column_by_name(): ")
        print (filter_jdbc_table.select_column_by_name(filterID_jdbc_column.name)) ## ?? Not implemented yet
        print ("create_column(): ")
        print (filter_jdbc_table.create_column("myColumn")) ## ?? Not implemented yet
    
        
        # Jdbc Column Tests
        
        print ("url: " + filterID_jdbc_column.url)
        print ("name(): " + filterID_jdbc_column.name())
        print ("ident(): " + filterID_jdbc_column.ident())
        print ("owner(): " + filterID_jdbc_column.owner())
        print ("resource(): ")
        print (filterID_jdbc_column.resource())
        print ("schema(): ")
        print (filterID_jdbc_column.schema())
        print ("table(): ")
        print (filterID_jdbc_column.table())
        print ("type(): " + filterID_jdbc_column.type())
        print ("arraysize(): " + str(filterID_jdbc_column.size()))
        print ("ucd(): " + str(filterID_jdbc_column.ucd()))
        print ("utype(): " + str(filterID_jdbc_column.utype()))
    
        # Create an IvoaResource to represent the GAIA TAP resource.
        tapname="GAIA TAP service"
        tapurl="http://gea.esac.esa.int/tap-server/tap"
        vosifile='https://raw.githubusercontent.com/stvoutsin/firethorn.py/master/firethorn/meta/vosi/gaia/gaia-tableset.xml'
        gaia_ivoa_resource = ft.firethorn_engine.create_ivoa_resource(tapname, tapurl)
        gaia_ivoa_resource.import_ivoa_metadoc(vosifile)
        
        print ("gaia_ivoa_resource: ")
        print (gaia_ivoa_resource)
        print (ft.firethorn_engine.select_ivoa_resource_by_ident(gaia_ivoa_resource.url))    
        print (ft.firethorn_engine.select_ivoa_resource_by_name(None)) ## Not implemented yet
        print (gaia_ivoa_resource.select_schemas())
        print (gaia_ivoa_resource.select_schema_by_ident(gaia_ivoa_resource.select_schemas()[0].get("self")))
        print (gaia_ivoa_resource.select_schema_by_name("gaiadr1"))
                

        # Test IVOA Schema
        ivoa_schema = gaia_ivoa_resource.select_schema_by_ident(gaia_ivoa_resource.select_schemas()[0].get("self"))
        print (ivoa_schema.name())
        print (ivoa_schema.resource())
        print (ivoa_schema.schema_name())
        print (ivoa_schema.select_tables())
        ivoa_table = ivoa_schema.select_table_by_ident(ivoa_schema.select_tables()[0].get("self"))
        print (ivoa_table)
        print (ivoa_schema.select_table_by_name(ivoa_table.name))


        # Test IVOA Table
        print (ivoa_table)
        print (ivoa_table.name())
        print (ivoa_table.select_columns())
        ivoa_column = ivoa_table.select_column_by_ident(ivoa_table.select_columns()[0].get("self"))
        print (ivoa_column)
        print (ivoa_table.select_column_by_name(ivoa_column.name))
        
        # Create an empty AdqlResource to represent the local JDBC database.
        adqlname="ATLAS ADQL resource"
        atlas_adql = ft.firethorn_engine.create_adql_resource(adqlname)
        print ("atlas_adql: ")
        print (atlas_adql) 
        print (ft.firethorn_engine.select_adql_resource_by_ident(atlas_adql.url))
        print (ft.firethorn_engine.select_adql_resource_by_name("ATLAS")) ## ?? Not implemented yet
    
    
        
        # Import the mapping between JDBC and ADQL tables.
        metadoc="https://raw.githubusercontent.com/stvoutsin/firethorn.py/master/firethorn/meta/ATLASDR1_TablesSchema.xml"
        atlas_adql_schema = atlas_adql.import_jdbc_schema(atlas_jdbc_schema, metadoc=metadoc)
        atlas_adql_schemav2 = atlas_adql.import_jdbc_schema(atlas_jdbc_schema, schema_name="atlasV2")
        print (atlas_adql_schema)
        print (atlas_adql_schemav2)
        print (atlas_adql.select_schema_by_ident(atlas_adql_schema.url))
        print (atlas_adql.select_schema_by_name("ATLASDR1"))
        print (atlas_adql.select_schemas())
        
    
    
        # Create an empty AdqlResource to represent a Query Resource
        adqlname="Query resource"
        query_resource = ft.firethorn_engine.create_adql_resource(adqlname)
        print ("query_resource: ")
        print (query_resource)
        my_atlas_schema = query_resource.import_adql_schema(atlas_adql_schema, "myAtlas")
        query_resource.import_adql_schema(atlas_adql_schema)
        query_resource.import_ivoa_schema(ivoa_schema, "gaiadr1")
        print(query_resource.create_adql_schema("mySchema"))
        print (query_resource.select_schemas())
        
        
        # ADQL Schema Tests    
        print ("myAtlas Schema: ")
        print (my_atlas_schema)
        print ("resource(): ")
        print (my_atlas_schema.resource())
        print ("Name: " + my_atlas_schema.name())
        print ("Owner: " + my_atlas_schema.owner())
        print ("select_tables(): ")
        print (my_atlas_schema.select_tables())
        print ("select_table_by_ident(): ")
        print (my_atlas_schema.select_table_by_ident(my_atlas_schema.select_tables()[0].get("self")))
        print ("select_table_by_name(): ") 
        my_adql_table = my_atlas_schema.select_table_by_name("Filter")
        print (my_adql_table)
        print ("create_table(): ") 
        print (my_atlas_schema.create_table("myTable")) ## ???? Not implemented yet
        my_atlas_schema.import_ivoa_table(None) ## ???? Not implemented yet
        my_atlas_schema.import_adql_table(None) ## ???? Not implemented yet
        
        
        # ADQL Table Tests    
        print ("name(): " + my_adql_table.name())
        print ("ident(): " + my_adql_table.ident())
        print ("resource(): ") 
        print (my_adql_table.resource())
        print ("schema(): ") 
        print (my_adql_table.schema())
        print ("select_columns(): ") 
        print (my_adql_table.select_columns())
        print ("select_column_by_ident(): ")
        my_column = my_adql_table.select_column_by_ident(my_adql_table.select_columns()[0].get("self"))
        print (my_column)
        print ("select_column_by_name(): ")
        print (my_adql_table.select_column_by_name(my_column.name)) ## ?? Not implemented yet
        print ("create_column(): ")
        print (my_adql_table.create_adql_column("myColumn")) ## ?? Not implemented yet    
    
    
        # ADQL Column Tests
        print ("url: " + my_column.url)
        print ("name(): " + my_column.name())
        print ("ident(): " + my_column.ident())
        print ("owner(): " + my_column.owner())
        print ("resource(): ")
        print (my_column.resource())
        print ("schema(): ")
        print (my_column.schema())
        print ("table(): ")
        print (my_column.table())
        print ("type(): " + my_column.type())
        print ("arraysize(): " + str(my_column.arraysize()))
        print ("ucd(): " + str(my_column.ucd()))
        print ("utype(): " + str(my_column.utype()))



        
        # List the workspace schema.
        print (atlas_adql.select_schemas())


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()