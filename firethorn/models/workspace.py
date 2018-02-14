import logging
from models.query import Query, AsyncQuery
from core.firethorn_engine import FirethornEngine
import models.adql as adql

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

    def __init__(self, adql_resource=None, ident=None, url=None, firethorn_engine=None):
        self.firethorn_engine = firethorn_engine
        self.ident = ident
        self.url = url             
        self.adql_resource = adql_resource
        return        


    @property
    def ident(self):
        return self.__ident
        
        
    @ident.setter
    def ident(self, ident):
        self.__ident = ident 


    @property
    def url(self):
        return self.__url
                

    @url.setter
    def url(self, url):
        self.__url = url 


    @property
    def adql_resource(self):
        return self.__adql_resource

                
    @adql_resource.setter
    def adql_resource(self, adql_resource):
        self.__adql_resource = adql_resource    

    
    def import_schema(self, schema_name=None, adql_schema=None):
        """
        Import a Schema into the workspace
        """
        
        if adql_schema==None:
            adql_schema = self.adql_resource.select_adql_schema_by_name(schema_name)
      
        self.adql_resource.import_adql_schema(adql_schema, schema_name)

    
    def get_schema(self, schema_name=None):
        """
        Get a copy of the schema by name
        """
        adql_schema = self.adql_resource.select_adql_schema_by_name(schema_name)
        return adql_schema
    
    
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
             
        query = Query(query, self.adql_resource, firethorn_engine = self.firethorn_engine)
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
             
        return AsyncQuery(query, self.adql_resource, firethorn_engine = self.firethorn_engine)

