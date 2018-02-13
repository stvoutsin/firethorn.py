import logging
from models.query import Query, AsyncQuery
from core.firethorn_engine import FirethornEngine


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

    def __init__(self, ident=None, url=None, firethorn_engine=None):
        self.firethorn_engine = firethorn_engine
        self.ident = ident
        self.url = url             
        return        


    @property
    def ident(self):
        return self.__ident
        
        
    @ident.setter
    def ident(self, ident):
        self.__ident = ident 
        self.firethorn_engine.adqlspace = ident


    @property
    def url(self):
        return self.__url
        
        
    @url.setter
    def queryspace(self, url):
        self.__url = url


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
             
        query = Query(query, self.queryspace, firethorn_engine = self.firethorn_engine)
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
             
        return AsyncQuery(query, self.url, firethorn_engine = self.firethorn_engine)

