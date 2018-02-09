'''
Created on Feb 8, 2018

@author: stelios
'''

import urllib
import json
import logging
import core.auth_engine
import os

class BaseObject(object):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None ):
        """
        Constructor
        """
        self.json_object = json_object
        self.url = url
        self.firethorn_engine = firethorn_engine
        if (self.json_object==None and self.url!=None):
            self.json_object = self.firethorn_engine.get_json(self.url) 
             
        
        
    @property
    def url(self):
        return self.__url
        
        
    @url.setter
    def url(self, url):
        self.__url= url 
        if (self.json_object!=None and url==None):
            self.__url = self.json_object.get("self","")           


    def name(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("name","")
        else:
            return self.json_object.get("name","")
        
        
    def ident(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return os.path.basename(self.json_object.get("self",""))
        else:
            return os.path.basename(self.json_object.get("self",""))
        
        
    def owner(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("owner","")
        else:
            return self.json_object.get("owner","")


    def refresh(self):
        self.json_object = self.firethorn_engine.get_json(self.url)
      

