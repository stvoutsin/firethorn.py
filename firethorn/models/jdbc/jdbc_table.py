'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_table import BaseTable

class JdbcTable(BaseTable):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        
    
    def create_column(self, column_name):
        return