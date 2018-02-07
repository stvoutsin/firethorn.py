'''
Created on Feb 8, 2018

@author: stelios
'''

class BaseObject(object):
    """
    classdocs
    """


    def __init__(self, resource_json=None):
        """
        Constructor
        """
        
    def url(self):
        if (self.resource_json):
            return self.resource_json.getAttribute("url","")
        

    def name(self):
        if (self.resource_json):
            return self.resource_json.getAttribute("name","")
        
        
    def ident(self):
        if (self.resource_json):
            return self.resource_json.getAttribute("ident","")
        
        
    def owner(self):
        if (self.resource_json):
            return self.resource_json.getAttribute("owner","")


    def refresh(self):
        return
