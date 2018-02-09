'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_table import BaseTable


class AdqlTable(BaseTable):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
    
    
    def select_columns(self):
        return
    
    
    def select_column_by_ident(self, ident):
        return
    
    
    def select_column_by_name(self, column_name):
        return
   
   
    def create_adql_column(self, column_name):
        return
    
    