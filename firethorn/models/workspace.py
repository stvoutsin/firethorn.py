import logging
from models.query import Query, AsyncQuery
from core.firethorn_engine import FirethornEngine
from schema import Schema

class Workspace(object):
    """
    Workspace client class
         
    Attributes:
    """

    __id__ = ""
    
    
    def __init__(self, ident=None, queryspace=None):
        '''
        Constructor
        
        Parameters
        ----------
        '''
        self.firethorn_engine = FirethornEngine()
        self.ident = ident
        self.queryspace = queryspace             
        return        


    @property
    def ident(self):
        return self.__ident
        
        
    @ident.setter
    def ident(self, ident):
        self.__ident = ident 
        self.firethorn_engine.adqlspace = ident


    @property
    def queryspace(self):
        return self.__queryspace
        
        
    @queryspace.setter
    def queryspace(self, queryspace):
        self.__queryspace = queryspace


    def query(self, query=""):
        """        
        Run a query on the imported resources
        
        Parameters
        ----------
        query : str, required
            The query string
            
        Returns
        -------
        query : `Query`
        """
        
        try:
            if (not self.queryspace):
                self.queryspace = self.firethorn_engine.create_query_schema(self.ident)
        except Exception as e:
            logging.exception(e)   
             
        query = Query(query, self.queryspace)
        query.run()
        return query
    
    
    def query_async(self, query=""):
        """        
        Run am Asynchronous query on the imported resources
        
        Parameters
        ----------
        query : str, required
            The query string
            
        Returns
        -------
        query : `Query`
        """
        
        try:
            if (not self.queryspace):
                self.queryspace = self.firethorn_engine.create_query_schema(self.ident)
        except Exception as e:
            logging.exception(e)   
             
        return AsyncQuery(query, self.queryspace)


    def get_schema(self, name=""):
        """        
        Import a schema into this workspace
        
        Parameters
        ----------
        schema : str, required
            The URL for the schema to import 
            
        Returns
        -------
        """
        
        return Schema(self.firethorn_engine.select_by_name(name, self.ident), name, self.ident)
        

    def import_schema(self, schema):
        """        
        Import a schema into this workspace
        
        Parameters
        ----------
        schema : str, required
            The URL for the schema to import 
            
        Returns
        -------
        """
        
        self.firethorn_engine.import_query_schema(schema.name, schema.ident, self.ident)
        
        
    def get_tables(self, schemaname):
        """        
        Get list of tables
        
        Parameters
        ----------
        schema : str, required
            The parent Schema name 
            
        Returns
        -------
        """     
        return self.firethorn_engine.get_tables(schemaname)