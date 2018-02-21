'''
Created on Nov 4, 2017

@author: stelios
'''
import logging
from table import Table
from core.query_engine import QueryEngine
import urllib.request
from models.adql import adql_resource
import time
try:
    import simplejson as json
except ImportError:
    import json
import config as config


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

    def __init__(self, firethorn_engine=None, querystring=None,  adql_query=None):
        self.table = Table()
        self.adql_query = adql_query
        self.firethorn_engine = firethorn_engine
        pass
       
            
            
    @property
    def querystring(self):
        return self.__querystring
        
        
    @querystring.setter
    def querystring(self, querystring):
        self.__querystring = querystring
        
        
    @property
    def adql_resource(self):
        return self.__adql_resource
        
        
    @adql_resource.setter
    def adql_resource(self, adql_resource):
        self.__adql_resource = adql_resource    
        
    
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
        try: 
            if not self.table.tableident:
                self.table = Table(Query._get_json(self, self.adql_query.url).get("results",[]).get("table",None), firethorn_engine=self.firethorn_engine)
        except Exception as e:
            logging.exception(e)    

        return self.table



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

    def __init__(self, firethorn_engine=None, querystring=None, adql_query=None):
        super().__init__(firethorn_engine, querystring, adql_query)
        
                     
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
    
        