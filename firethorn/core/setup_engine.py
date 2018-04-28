'''
Created on Jul 22, 2013

@author: stelios
'''

try:
    
    import urllib.request
    import firethorn
    import json    
    import logging
except Exception as e:
    logging.exception(e)


class SetupEngine(object):
    """ Provides the low level methods to setup Firethorn services, including JDBC connections and importing IVOA or local resources
    """


    def __init__(self, json_file="", firethorn_base="", tap=True):
        self.json_file = json_file
        self.tap = tap
        self.jdbc_resources = {}
        self.ft = firethorn.Firethorn(endpoint=firethorn_base)
        self.ft.login(firethorn.config.adminuser, firethorn.config.adminpass, firethorn.config.admingroup)
    
    
    def load_jdbc_resources(self, jdbc_resources_json):
                
        for jdbc_resource in jdbc_resources_json:
            _id = jdbc_resource.get("id","")
            name = jdbc_resource.get("name","")
            jdbc_name = name   
            jdbc_resource["jdbc_object"] = self.ft.firethorn_engine.create_jdbc_resource(jdbc_name, jdbc_resource["datadata"], jdbc_resource["datacatalog"], jdbc_resource["datatype"], jdbc_resource["datahost"], jdbc_resource["datauser"], jdbc_resource["datapass"])
            self.jdbc_resources[_id] = jdbc_resource
  
        
    def create_adql_resource(self, resource):
                     
        name = resource.get("name","")
        _id = resource.get("id","")
        adql_schemas = resource.get("Schemas","")
        
        tap_name= name + " ADQL resource"
        new_adql_resource = self.ft.firethorn_engine.create_adql_resource(tap_name)
        
        for schema in adql_schemas:
            schema_name = schema.get("adqlschema")
            print ("Importing " + schema_name)
            jdbc_resource_dict = self.jdbc_resources.get(schema.get("jdbcid"))
            jdbc_resource_object = jdbc_resource_dict.get("jdbc_object")
            jdbc_schema = jdbc_resource_object.select_schema_by_name(
                schema.get("jdbccatalog"),
                schema.get("jdbcschema")
            )
            
            if (jdbc_schema!=None):
                metadoc = schema.get("metadata").get("metadoc")
                metadoc_catalog_name = schema.get("metadata").get("catalog")
                adql_schema = new_adql_resource.import_jdbc_schema(
                    jdbc_schema,
                    metadoc_catalog_name,
                    metadoc=metadoc
                    )
                
        return new_adql_resource

    def create_tap_service(self, new_adql_resource):
        req = urllib.request.Request( self.ft.endpoint + "/tap/"+ new_adql_resource.ident() + "/generateTapSchema", headers=new_adql_resource.account.get_identity_as_headers())
        response = urllib.request.urlopen(req)
        response.close()

    
    def setup_resources(self):
        with open(self.json_file) as json_data:
            json_obect = json.load(json_data)
            name = json_obect.get("name")
            adql_resources_json = json_obect.get("AdqlResources")
            jdbc_resources_json = json_obect.get("JdbcResources")

        self.load_jdbc_resources(jdbc_resources_json)
        for resource in adql_resources_json:
            new_adql_resource = self.create_adql_resource(resource)
            if (self.tap):
                self.create_tap_service(new_adql_resource)        



if __name__ == "__main__":
    sEng = SetupEngine(json_file="../data/osa-tap.json", firethorn_base="http://gworewia.metagrid.xyz/firethorn")
    sEng.setup_resources()
