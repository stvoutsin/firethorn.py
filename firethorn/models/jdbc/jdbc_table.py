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


    def __init__(self, jdbc_schema, json_object=None, url=None):
        """
        Constructor
        """
        self.jdbc_schema = jdbc_schema
        super().__init__(jdbc_schema, json_object, url) 
        
        
    def resource(self):
        if (self.jdbc_schema!=None):
            return self.jdbc_schema.resource()
        else:
            return None 
    
    
    def schema(self):
        #if (self.json_object!=None):
        #    return jdbc.JdbcSchema(adql_table=self, url=self.json_object.get("schema",""))
        #else:
        #    return None 
        return self.jdbc_schema
        
        
    def select_columns(self):
        column_list = []
        json_list = self.get_json(self.json_object.get("columns",""))

        for column in json_list:
            column_list.append(jdbc.JdbcColumn(json_object=column, jdbc_table=self))
            
        return column_list
    
    
    def select_column_by_ident(self, ident):
        return jdbc.JdbcColumn(jdbc_table=self, url=ident)
 
    
    def select_column_by_name(self, column_name):
        return 
                         
                         
    def create_column(self, column_name):
        return
    
    