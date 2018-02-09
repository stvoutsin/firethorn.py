'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_resource import BaseResource
from models.jdbc.jdbc_schema import JdbcSchema
import urllib
import config as config
import json
import logging

class JdbcResource(BaseResource):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        

    def select_schemas(self):
        return self.firethorn_engine.get_json(self.url + "/schemas/select")
    
    
    def select_schema_by_ident(self, ident):
        return JdbcSchema(url=ident, firethorn_engine=self.firethorn_engine)
    
    
    def select_schema_by_name(self, catalog_name, schema_name):
        response_json = {}
        try :
            data = urllib.parse.urlencode({config.jdbc_schema_catalog : catalog_name, config.jdbc_schema_schema : schema_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/schemas/select", headers=self.firethorn_engine.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return JdbcSchema(json_object = response_json, firethorn_engine=self.firethorn_engine)
    
    
    def create_schema(self, catalog_name, schema_name):
        return
    
    