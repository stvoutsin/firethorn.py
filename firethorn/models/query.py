'''
Created on Nov 4, 2017

@author: stelios
'''
import logging
from table import Table
try:
    import simplejson as json
except ImportError:
    import json


class Query(object):
    """Query Class, stores information for a Firethorn query 
    
    Attributes
    ----------
    querystring: string, optional
        The Query as a sring

    adql_resource: string, optional
        The query Resource
    
    adql_query: string, optional
        The AdqlQuery object
        
    firethorn_engine: FirethornEngine, optional
        The Firethorn Engine currently driving this query   
                  
    """

    def __init__(self, auth_engine=None, querystring=None,  adql_query=None):
        self.adql_query = adql_query
        self.querystring = querystring
        if (self.auth_engine==None and self.adql_query!=None):
            self.auth_engine = self.adql_query.auth_engine
        pass
       
            
    @property
    def querystring(self):
        return self.__querystring
        
        
    @querystring.setter
    def querystring(self, querystring):
        self.__querystring = querystring
        
    
    @property
    def adql_query(self):
        return self.__adql_query
        
        
    @adql_query.setter
    def adql_query(self, adql_query):
        self.__adql_query = adql_query    
        
                     
    def run (self):
        """Run a query
        
        """
        try: 
            self.adql_query.run_sync()
        except Exception as e:
            logging.exception(e)
        return 
    

    def results (self):
        """Get Results
        
        """
        if (self.adql_query!=None):
            if (self.adql_query.table()!=None):
                return Table(table=self.adql_query.table(), auth_engine=self.auth_engine)
        
        return None


    def status (self):
        """Get Status 
        """
        return self.adql_query.status()

                   
    def error (self):
        """Get Error message
        """
        return self.adql_query.get_error()

                        
    def __str__(self):
        """ Print class as string
        """
        return 'Query: %s\nQuery ID: %s\n ' %(self.querystring, self.adql_query.url) 
    


class AsyncQuery(Query):
    """AsyncQuery Model, stores information for a Firethorn query 
    
    Attributes
    ----------
    querystring: string, optional
        The Query as a sring

    adql_query: string, optional
        The AdqlQuery object
                  
                    
    """

    def __init__(self, auth_engine=None, querystring=None, adql_query=None):
        super().__init__(auth_engine=auth_engine, querystring=querystring, adql_query=adql_query)
        
                     
    def run (self):
        """
        Run Query
        """
        try: 
            self.adql_query.update(adql_query_status_next="COMPLETED")
        except Exception as e:
            logging.exception(e)    
            
        return 
    

    def update (self, query_input):
        """
        Run Query
        """
        try: 
            self.adql_query.update(adql_query_input=query_input)
        except Exception as e:
            logging.exception(e)    
            
        return 
    

    def __str__(self):
        """ Print Class as string
        """
        return 'Query: %s\nQuery URL: %s\n ' %(self.querystring, self.adql_query.url) 
    
        