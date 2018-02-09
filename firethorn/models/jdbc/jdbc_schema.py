'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema
import jdbc
import urllib
import json
import logging
import config as config

class JdbcSchema(BaseSchema):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
    
    
    def resource(self):
        if (self.json_object!=None):
            return jdbc.JdbcResource(firethorn_engine=self.firethorn_engine, url=self.json_object.get("parent",""))
        else:
            return None 
    
        
    def catalog_name(self):
        if (self.json_object!=None):
            return self.json_object.get("fullname","")
        else:
            return None
    
    
    def schema_name(self):
        if (self.json_object!=None):
            return self.json_object.get("fullname","")
        else:
            return None
                   
    
    def select_tables(self):
        return self.firethorn_engine.get_json(self.json_object.get("tables",""))
    
        
    def select_table_by_ident(self, ident):
        return jdbc.JdbcTable(firethorn_engine=self.firethorn_engine, url=ident)
    
    
    def select_table_by_name(self, table_name):
        """Get table by name
        
        Parameters
        ----------
        table_name: string, required
            The name of the Table being searched
         
        Returns
        -------
        table_list: list
            List of table names
        """
        response_json = {}
        try :
            data = urllib.parse.urlencode({ "jdbc.table.name": table_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/tables/select", headers=self.firethorn_engine.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            #logging.exception(e)   
            print (e)   
            return   
        return jdbc.JdbcTable(json_object = response_json, firethorn_engine=self.firethorn_engine)    
    
                           
    def create_table(self, table_name):
        return
    
