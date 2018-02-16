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

    def __init__(self, querystring=None, adql_resource=None, adql_query=None, firethorn_engine=None):
        self.table = Table()
        self.error = None
        self.querystring = querystring
        self.adql_resource = adql_resource
        self.firethorn_engine = firethorn_engine
        self.firethorn_query_engine = QueryEngine(firethorn_engine)
        self.adql_query = adql_query
        pass
       
       
    def _get_json(self, url): 
        """Get request to a JSON service
        
        Parameters
        ----------
        url: string, required
            JSON Web Resource URL
            
        Returns    
        -------
        query_json: json
            JSON object returned by GET request
        
        """
        query_json=None
        request=None
        try:
            request = urllib.request.Request(url, headers=self.firethorn_engine.identity.get_identity_as_headers())
            with urllib.request.urlopen(request) as response:
                query_json =  json.loads(response.read().decode('UTF-8'))
        except Exception as e:
            logging.exception(e)
        return query_json   
            
            
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
            self.adql_query = self.firethorn_query_engine.run_query(self.querystring, "", self.adql_resource.url, "AUTO", config.test_email, "SYNC")
            while self.adql_query.status()=="RUNNING" or self.adql_query.status()=="READY":
                time.sleep(5)
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
        """Get Status message for query
        """
        
        status = "UNKNOWN"
        
        try:
            statusjson = self.firethorn_query_engine.get_status(self.adql_query.url)
            status = json.loads(statusjson).get("status","UNKNOWN")
        except Exception as e:
            logging.exception(e)
        
        return status
                   
                   
    def get_error (self):
        """Get Error message
        """
        try: 
            if not self.error:
                self.error = Query._get_json(self, self.adql_query.url).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error

                        
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

    adql_resource: string, optional
        The query Resource
    
    adql_query: string, optional
        The AdqlQuery object
        
    firethorn_engine: FirethornEngine, optional
        The Firethorn Engine currently driving this query   
                  
                    
    """

    def __init__(self, querystring=None, adql_resource=None, adql_query=None, firethorn_engine = None):
        super().__init__(querystring, adql_resource, adql_query, firethorn_engine=firethorn_engine)

        try: 
            self.adql_query = self.firethorn_query_engine.create_query( self.querystring, None, adql_resource, firethorn_engine)
        except Exception as e:
            logging.exception(e)    
        
        return 
        
                     
    def run (self):
        """
        Run Query
        """
        try: 
            self.firethorn_query_engine.update_query(adql_resource=self.adql_resource, firethorn_engine=self.firethorn_engine, adql_query_status_next="COMPLETED")
        except Exception as e:
            logging.exception(e)    
            
        return 
    

    def update (self, query_input):
        """
        Run Query
        """
        try: 
            self.firethorn_query_engine.update_query(adql_resource=self.adql_resource, firethorn_engine=self.firethorn_engine, adql_query_input=query_input)
        except Exception as e:
            logging.exception(e)    
            
        return 
    
    
    def results (self):
        """
        Get Results
        """
        try: 
            if not self.table.tableident:
                self.table = Table(Query._get_json(self, self.adql_query.url).get("results",[]).get("table",None),firethorn_engine = self.firethorn_engine)
        except Exception as e:
            logging.exception(e)    

        return self.table



    def status (self):
        """
        Get Status message for query
        """
        
        status = "UNKNOWN"
        
        try:
            statusjson = self.firethorn_query_engine.get_status(self.adql_query.url)
            status = json.loads(statusjson).get("status","UNKNOWN")
        except Exception as e:
            logging.exception(e)
        
        return status
                   
                   
    def get_error (self):
        """
        Get Error message
        """
        try: 
            if not self.error:
                self.error = Query._get_json(self, self.adql_query.url).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error

                        
    def __str__(self):
        """ Print Class as string
        """
        return 'Query: %s\nQuery URL: %s\n ' %(self.querystring, self.adql_query.url) 
    
        