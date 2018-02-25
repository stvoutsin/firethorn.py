'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_schema import BaseSchema
import adql
import logging


class AdqlSchema(BaseSchema):
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
            return adql.AdqlResource(auth_engine=self.auth_engine, url=self.json_object.get("parent",""))
        else:
            return None 
        
        
    def select_tables(self):
        table_list = []
        json_list = self.get_json(self.json_object.get("tables",""))

        for table in json_list:
            table_list.append(adql.AdqlTable(json_object=table, auth_engine=self.auth_engine))
            
        return table_list
    
        
    def select_table_by_ident(self, ident):
        return adql.AdqlTable(auth_engine=self.auth_engine, url=ident)
    
    
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
            response_json = self.get_json( self.url + "/tables/select", { "adql.table.name": table_name })
        except Exception as e:
            logging.exception(e)   
            
        return adql.AdqlTable(json_object = response_json, auth_engine=self.auth_engine)    
    
                           
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
    