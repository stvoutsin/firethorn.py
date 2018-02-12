'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema
import adql
import urllib
import json
import config as config

class AdqlSchema(BaseSchema):
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
            return adql.AdqlResource(firethorn_engine=self.firethorn_engine, url=self.json_object.get("parent",""))
        else:
            return None 
        
        
    def select_tables(self):
        return self.firethorn_engine.get_json(self.json_object.get("tables",""))
    
        
    def select_table_by_ident(self, ident):
        return adql.AdqlTable(firethorn_engine=self.firethorn_engine, url=ident)
    
    
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
            data = urllib.parse.urlencode({ "adql.table.name": table_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/tables/select", headers=self.firethorn_engine.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            #logging.exception(e)   
            print (e)   
            
        return adql.AdqlTable(json_object = response_json, firethorn_engine=self.firethorn_engine)    
    
                           
    def create_table(self, table_name):
        return                             

    
    def import_ivoa_table(self, IvoaTable, table_name=None):
        return
 
    
    def import_jdbc_table(self, JdbcTable, table_name=None) :           
        return


    def import_adql_table(self, AdqlTable, table_name=None):
        return          

                   
    def create_adql_table(self, table_name=None):
        return
    