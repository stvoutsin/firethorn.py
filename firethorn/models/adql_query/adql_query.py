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
    import core as core
    import adql
    import time
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
        self.query_engine = core.query_engine.QueryEngine(self.firethorn_engine)
        
        
    def ident(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return os.path.basename(self.json_object.get("self",""))
        else:
            return os.path.basename(self.json_object.get("self",""))
         
        
    def resource(self):
        if (self.json_object!=None):
            return adql.AdqlResource(firethorn_engine=self.firethorn_engine, url=self.json_object.get("workspace",""))
        else:
            return None 
        
        
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


    def error (self):
        """Get Error message
        """
        try: 
            error = self.adql_query.getAttr("syntax").get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return error

        
    def status(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("status","").upper()
        else:
            return self.json_object.get("status","").upper()
        
        
    def results(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("results","")
        else:
            return self.json_object.get("results","")


    def table(self):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get("results","").get("table",None)
        else:
            return self.json_object.get("results","").get("table",None)  
        
        
    def getAttr(self, attribute):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.firethorn_engine.get_json(self.url)
                return self.json_object.get(attribute,"")
        else:
            return self.json_object.get(attribute,"")
        
    
    def update(self, adql_query_input=None, adql_query_status_next=None, adql_query_wait_time=None):
        return self.query_engine.update_query(adql_query_input=adql_query_input, adql_query_status_next=adql_query_status_next, adql_query=self, firethorn_engine=self.firethorn_engine, adql_query_wait_time=adql_query_wait_time)


    def run_sync(self):
        self.query_engine.run_query(self.adql(), "", self.resource(), "AUTO", None, "SYNC")
        while self.status()=="RUNNING" or self.status()=="READY":
            time.sleep(3)
        
        return 
             
             
    def __str__(self):
        """ Print Class as string
        """
        return 'Query URL: %s' %(self.json_object.get("self",""))
