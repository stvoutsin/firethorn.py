'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_resource import BaseResource

class AdqlResource(BaseResource):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        
    
    def create_adql_schema(self, catalog_name, schema_name):
        return
    
    
    def import_ivoa_schema(self, IvoaSchema, schema_name=None):
        return
    
    
    def import_jdbc_schema(self, JdbcSchema, schema_name=None, metadoc=None):                  
        return
    
    
    def import_adql_schema(self, AdqlSchema, schema_name=None):                   
        return
    
    