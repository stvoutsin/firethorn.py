'''
Created on Feb 8, 2018

@author: stelios
'''
try:
    import logging
    from models.base.base_object import BaseObject
except Exception as e:
    logging.exception(e)
    

class Owner(BaseObject):
    """Owner class 
    """
        
    def __init__(self, auth_engine, json_object=None, url=None ):
        """
        Constructor
        """
        super().__init__(auth_engine, json_object, url) 
        
        
    def community(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.get_json(self.url)
                return self.json_object.get("community","")
        else:
            return self.json_object.get("community","")

        
    def text(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.get_json(self.url)
                return self.json_object.get("text","")
        else:
            return self.json_object.get("text","")
        
        
            
    def created(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.get_json(self.url)
                return self.json_object.get("created","")
        else:
            return self.json_object.get("created","")
        
        
    def modified(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.get_json(self.url)
                return self.json_object.get("modified","")
        else:
            return self.json_object.get("modified","")
        
        
    def __str__(self):
        """ Print Owner as string
        """
        return 'Owner name: %s ' %(self.name()) 

        