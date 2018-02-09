'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_object import BaseObject

class BaseResource(BaseObject):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        

    def select_schemas(self):
        return
    
    
    def select_schema_by_ident(self, ident):
        return
    
    
    def select_schema_by_name(self, catalog_name, schema_name):
        return


    def __str__(self):
        """ Print Class as string
        """
        return 'Resource URL: %s' %(self.json_object.get("self",""))