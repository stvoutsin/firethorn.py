import logging
from models.query import Query, AsyncQuery
from core.firethorn_engine import FirethornEngine
from schema import Schema

class Workspace(object):
    """
    Workspace client class
         
    Attributes
    ----------
      
    ident: string, optional
        The identity URL of the Workspace
        
    queryspace: string, optional
        The URL of the query schema of the workspace
        
    """

    def __init__(self, ident=None, queryspace=None, firethorn_engine=None):
        self.firethorn_engine = firethorn_engine
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
            The created Query
        """
        
        try:
            if (not self.queryspace):
                self.queryspace = self.ident
        except Exception as e:
            logging.exception(e)   
             
        query = Query(query, self.queryspace, user=self.firethorn_engine.user, firethorn_engine = self.firethorn_engine)
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
        query : `AsyncQuery`
            The created AsyncQuery
        """
        
        try:
            if (not self.queryspace):
                self.queryspace = self.ident
        except Exception as e:
            logging.exception(e)   
             
        return AsyncQuery(query, self.queryspace, user=self.firethorn_engine.user, firethorn_engine = self.firethorn_engine)


    def get_schema(self, name=""):
        """        
        Import a schema into this workspace
        
        Parameters
        ----------
        schema : str, required
            The URL for the schema to import 
            
        Returns
        -------
        Schema : `Schema`
            The Schema requested      

        """
        
        return Schema(self.firethorn_engine.select_by_name(name, self.ident), name, self.ident)
        

    def import_schema(self, schema):
        """        
        Import a schema into this workspace
        
        Parameters
        ----------
        schema : str, required
            The URL for the schema to import 
            
        """
        
        self.firethorn_engine.import_schema(schema.name, schema.ident, self.ident)
        
        
    def get_tables(self, schemaname):
        """        
        Get list of tables
        
        Parameters
        ----------
        schema : str, required
            The parent Schema name 
            
        Returns
        -------
        list: list
            List of Tables as strings
        """     
        return self.firethorn_engine.get_tables(schemaname)