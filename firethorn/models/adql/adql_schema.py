'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema

class AdqlSchema(BaseSchema):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        
        
    def select_tables(self):
        return
    
    
    def select_table_by_ident(self, ident):
        return
    
    
    def select_table_by_name(self, table_name):
        return
   
   
    def import_ivoa_table(self, IvoaTable, table_name=None):
        return
 
    
    def import_jdbc_table(self, JdbcTable, table_name=None) :           
        return


    def import_adql_table(self, AdqlTable, table_name=None):
        return          

                   
    def create_adql_table(self, table_name=None):
        return
    