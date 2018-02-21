'''
Created on Feb 21, 2018

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
    import adql
    import time
except Exception as e:
    logging.exception(e)
    
    
class AdqlTable(BaseObject):
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
         
        
    def resource(self):
        if (self.json_object!=None):
            return adql.AdqlResource(firethorn_engine=self.firethorn_engine, url=self.json_object.get("parent",""))
        else:
            return None 

        
    def getAttr(self, attribute):
        if (self.json_object==None):
            if (self.url!=None):
                self.json_object = self.get_json(self.url)
                return self.json_object.get(attribute,"")
        else:
            return self.json_object.get(attribute,"")
        

    def get_error (self):
        """Get Error message
        
        Returns
        -------
        error: string
            Error Message 
        """        
        error = None
        try: 
            error = self.json_object.get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return error


    def __get_json(self, url): 
        """Get request to a JSON service
        
        Parameters
        ----------
        url: string, required
            JSON Web Resource URL
            
        Returns    
        -------
        query_json: json
            JSON object returned by GET request
        
        """

        query_json=[]
        try:
            request = urllib.request.Request(url, headers=self.firethorn_engine.identity.get_identity_as_headers())
            with urllib.request.urlopen(request) as response:
                query_json = json.loads(response.read().decode('utf-8'))            
        except Exception as e:
            logging.exception(e)
        return query_json    
             
             
    def __get_votable(self, url): 
        """Get request to a service that returns a VOTable
        
        Parameters
        ----------
        url: string, required
            VOTable web Resource URL
            
        Returns    
        -------
        query_xml: string
            XML as string returned by GET request
        
        """

        query_xml=""
        request=None
        try:
            request = urllib.request.Request(url, headers=self.firethorn_engine.identity.get_identity_as_headers())
            with urllib.request.urlopen(request) as response:
                query_xml =  response.read().decode('utf-8')   
        except Exception as e:
            logging.exception(e)
        return query_xml  
        
             
    def __str__(self):
        """ Print Class as string
        """
        return 'Table URL: %s' %(self.json_object.get("self",""))
