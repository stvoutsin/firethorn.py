'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_object import BaseObject

class BaseTable(BaseObject):
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
    
    
    def schema(self):
        return
    
    
    def __str__(self):
        """ Print Class as string
        """
        return 'Table URL: %s' %(self.json_object.get("self",""))