'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema

class JdbcSchema(BaseSchema):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        
        
    def catalog_name(self):
        return
    
    
    def schema_name(self):
        return
                   
                   
    def create_table(self, table_name):
        return