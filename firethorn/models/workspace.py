import logging
from models.query import Query, AsyncQuery
from core.firethorn_engine import FirethornEngine

class Workspace(object):
    """
    Workspace client class
         
    Attributes:
    """

    __id__ = ""
    
    
    def __init__(self, resource=None, queryspace=None):
        '''
        Constructor
        
        Parameters
        ----------
        '''
        self.resource = resource
        self.queryspace = queryspace             
        return        


    @property
    def ident(self):
        return self.__ident
        
        
    @ident.setter
    def ident(self, ident):
        self.__ident = ident


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
                fEng = FirethornEngine()
                self.queryspace = fEng.create_query_schema(self.resource)
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
                fEng = FirethornEngine()
                self.queryspace = fEng.create_query_schema(self.resource)
        except Exception as e:
            logging.exception(e)   
             
        return AsyncQuery(query, self.queryspace)

