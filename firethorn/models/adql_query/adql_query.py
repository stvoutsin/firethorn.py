'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    import logging
    from models.base.base_object import BaseObject
    import urllib
    import json
    import config as config
    import os
    import pycurl
    import io
    import uuid
    import urllib.request
except Exception as e:
    logging.exception(e)
    
    
class AdqlQuery(BaseObject):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        
        
    def ident(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return os.path.basename(self.json_object.get("self",""))
        else:
            return os.path.basename(self.json_object.get("self",""))
         
        
    def osql(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("osql","")
        else:
            return self.json_object.get("osql","")
        
        
    def adql(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("adql","")
        else:
            return self.json_object.get("adql","")
        
        
    def status(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("status","")
        else:
            return self.json_object.get("status","")
        
        
    def results(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("results","")
        else:
            return self.json_object.get("results","")
        
        
    def getAttr(self, attribute):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get(attribute,"")
        else:
            return self.json_object.get(attribute,"")
        
    
    def __str__(self):
        """ Print Class as string
        """
        print (self.json_object)
        return 'Query URL: %s' %(self.json_object.get("self",""))
