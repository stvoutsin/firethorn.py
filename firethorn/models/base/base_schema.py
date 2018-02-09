'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_object import BaseObject

class BaseSchema(BaseObject):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        
        
    def resource(self):
        return 
    
    
    def select_tables(self):
        return
    

    def select_table_by_ident(self, ident):
        return
    
    
    def select_table_by_name(self, table_name):
        return
    
    
    def __str__(self):
        """ Print Class as string
        """
        return 'Schema URL: %s' %(self.json_object.get("self",""))