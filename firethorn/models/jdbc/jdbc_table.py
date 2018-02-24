'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_table import BaseTable
import jdbc

class JdbcTable(BaseTable):
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
            return jdbc.JdbcResource(auth_engine=self.auth_engine, url=self.json_object.get("resource",""))
        else:
            return None 
    
    
    def schema(self):
        if (self.json_object!=None):
            return jdbc.JdbcSchema(auth_engine=self.auth_engine, url=self.json_object.get("schema",""))
        else:
            return None 
    
        
    def select_columns(self):
        column_list = []
        json_list = self.get_json(self.json_object.get("columns",""))

        for column in json_list:
            column_list.append(jdbc.JdbcColumn(json_object=column, auth_engine=self.auth_engine))
            
        return column_list
    
    
    def select_column_by_ident(self, ident):
        return jdbc.JdbcColumn(auth_engine=self.auth_engine, url=ident)
 
    
    def select_column_by_name(self, column_name):
        return 
                         
                         
    def create_column(self, column_name):
        return