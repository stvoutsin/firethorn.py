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


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        
        
    def resource(self):
        if (self.json_object!=None):
            return jdbc.JdbcResource(firethorn_engine=self.firethorn_engine, url=self.json_object.get("resource",""))
        else:
            return None 
    
    
    def schema(self):
        if (self.json_object!=None):
            return jdbc.JdbcSchema(firethorn_engine=self.firethorn_engine, url=self.json_object.get("schema",""))
        else:
            return None 
    
    """    
    def select_columns(self):
        return self.firethorn_engine.get_json(self.json_object.get("columns",""))
    """
    
    def select_column_by_ident(self, ident):
        return jdbc.JdbcColumn(firethorn_engine=self.firethorn_engine, url=ident)
 
    
    def select_column_by_name(self, column_name):
        return 
                         
                         
    def create_column(self, column_name):
        return