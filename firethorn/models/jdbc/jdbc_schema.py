'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema
import jdbc
import logging


class JdbcSchema(BaseSchema):
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
            return jdbc.JdbcResource(auth_engine=self.auth_engine, url=self.json_object.get("parent",""))
        else:
            return None 
    
        
    def catalog_name(self):
        if (self.json_object!=None):
            return self.json_object.get("fullname","")
        else:
            return None
    
    
    def select_tables(self):
        table_list = []
        json_list = self.get_json(self.json_object.get("tables",""))

        for column in json_list:
            table_list.append(jdbc.JdbcTable(json_object=column, auth_engine=self.auth_engine))
            
        return table_list
    
        
    def select_table_by_ident(self, ident):
        return jdbc.JdbcTable(auth_engine=self.auth_engine, url=ident)
    
    
    def select_table_by_name(self, table_name):
        """Get table by name
        
        Parameters
        ----------
        table_name: string, required
            The name of the Table being searched
         
        Returns
        -------
        JdbcTable: JdbcTable
            The JdbcTable found
        """
        response_json = {}
        try :
            response_json = self.get_json(self.url + "/tables/select", { "jdbc.table.name": table_name })                
        except Exception as e:
            logging.exception(e)   
            
        return jdbc.JdbcTable(json_object = response_json, auth_engine=self.auth_engine)    
    
                           
    def create_table(self, table_name):
        return
    
