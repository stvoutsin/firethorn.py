'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_table import BaseTable
import adql
import urllib
import json

class AdqlTable(BaseTable):
    """
    classdocs
    """


    def __init__(self, auth_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(auth_engine, json_object, url) 
    
        
    def resource(self):
        if (self.json_object!=None):
            return adql.AdqlResource(auth_engine=self.auth_engine, url=self.json_object.get("resource",""))
        else:
            return None 
    
    
    def schema(self):
        if (self.json_object!=None):
            return adql.AdqlSchema(auth_engine=self.auth_engine, url=self.json_object.get("schema",""))
        else:
            return None 
    
    
    def select_columns(self):
        column_list = []
        json_list = self.get_json(self.url + "/columns/select")

        for column in json_list:
            column_list.append(adql.AdqlColumn(json_object=column, auth_engine=self.auth_engine))
            
        return column_list
    
    
    def select_column_by_ident(self, ident):
        return adql.AdqlColumn(auth_engine=self.auth_engine, url=ident)
 
    
    def select_column_by_name(self, column_name):
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
            data = urllib.parse.urlencode({ "adql.column.name": column_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/columns/select", headers=self.auth_engine.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            #logging.exception(e)   
            print (e)   
            
        return adql.AdqlColumn(json_object = response_json, auth_engine=self.auth_engine)    
                   
    
    def create_adql_column(self, column_name):
        return
    
    