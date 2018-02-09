'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_object import BaseObject

class BaseColumn(BaseObject):
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
    
    
    def table(self):
        return     


    def type(self):
        return
    
    
    def arraysize(self):
        return
     
     
    def ucd(self):
        return       
    
    
    def utype(self):
        return
    
    
    def __str__(self):
        """ Print Class as string
        """
        return 'Column URL: %s' %(self.json_object.get("self",""))    
    
    