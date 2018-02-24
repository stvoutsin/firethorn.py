'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_object import BaseObject

class BaseTable(BaseObject):
    """
    classdocs
    """


    def __init__(self, auth_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(auth_engine, json_object, url) 
        
        
    def resource(self):
        return
    
    
    def schema(self):
        return
    
    
    def select_columns(self):
        return self.get_json(self.json_object.get("columns",""))
    
