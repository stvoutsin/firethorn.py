'''
Created on Nov 4, 2017

@author: stelios
'''
import logging
from table import Table
from core.query_engine import QueryEngine
import urllib.request
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
                    
    queryspace: string, optional
        The query parent resource

    queryident: string, optional
        The query object URL
        
    user: User, optional
        The owner of the query
        
    firethorn_engine: FirethornEngine, optional
        The Firethorn Engine currently driving this query   
                  
    """

    def __init__(self, querystring=None, queryspace=None, queryident=None, user=None, firethorn_engine=None):
        self.table = Table()
        self.error = None
        self.querystring = querystring
        self.queryspace = queryspace
        self.firethorn_engine = firethorn_engine
        self.firethorn_query_engine = QueryEngine(user)
        self.queryident = queryident
        self.user = user
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
            request = urllib.request.Request(url, headers=self.user.get_user_as_headers())
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
    def queryspace(self):
        return self.__queryspace
        
        
    @queryspace.setter
    def queryspace(self, queryspace):
        self.__queryspace = queryspace    
        
    
    @property
    def queryident(self):
        return self.__queryident
        
        
    @queryident.setter
    def queryident(self, queryident):
        self.__queryident = queryident    
        
                     
    def run (self):
        """Run a query
        
        """
        try: 
            self.queryident = self.firethorn_query_engine.run_query(self.querystring, "", self.queryspace, "AUTO", config.test_email, "SYNC")
        except Exception as e:
            logging.exception(e)    
                
        return 
    

    def results (self):
        """Get Results
        
        """
        try: 
            if not self.table.tableident:
                self.table = Table(Query._get_json(self, self.queryident).get("results",[]).get("table",None), firethorn_engine=self.firethorn_engine)
        except Exception as e:
            logging.exception(e)    

        return self.table



    def status (self):
        """Get Status message for query
        """
        
        status = "UNKNOWN"
        
        try:
            statusjson = self.firethorn_query_engine.get_status(self.queryident)
            status = json.loads(statusjson).get("status","UNKNOWN")
        except Exception as e:
            logging.exception(e)
        
        return status
                   
                   
    def get_error (self):
        """Get Error message
        """
        try: 
            if not self.error:
                self.error = Query._get_json(self, self.queryident).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error

                        
    def __str__(self):
        """ Print class as string
        """
        return 'Queryspace ID: %s \nQuery: %s\nQuery ID: %s\n ' %(self.queryspace, self.querystring, self.queryident) 
    


class AsyncQuery(Query):
    """AsyncQuery Model, stores information for a Firethorn query 
    
    Attributes
    ----------
    querystring: string, optional
        The Query as a sring
                    
    queryspace: string, optional
        The query parent resource

    queryident: string, optional
        The query object URL
    
    user: User, optional
        The owner of the query
        
    firethorn_engine: FirethornEngine, optional
        The Firethorn Engine currently driving this query
                    
    """

    def __init__(self, querystring=None, queryspace=None, queryident=None, user=None, firethorn_engine = None):
        super().__init__(querystring, queryspace, queryident, user=user)

        try: 
            self.queryident = self.firethorn_query_engine.create_query(self.querystring, "", self.queryspace, "AUTO", config.test_email)
        except Exception as e:
            logging.exception(e)    
        
        return 
        
                     
    def run (self):
        """
        Run Query
        """
        try: 
            self.firethorn_query_engine.update_query_status(self.queryident, "COMPLETED")
        except Exception as e:
            logging.exception(e)    
            
        return 
    

    def results (self):
        """
        Get Results
        """
        try: 
            if not self.table.tableident:
                self.table = Table(Query._get_json(self, self.queryident).get("results",[]).get("table",None),firethorn_engine = self.firethorn_engine)
        except Exception as e:
            logging.exception(e)    

        return self.table



    def status (self):
        """
        Get Status message for query
        """
        
        status = "UNKNOWN"
        
        try:
            statusjson = self.firethorn_query_engine.get_status(self.queryident)
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
                self.error = Query._get_json(self, self.queryident).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error

                        
    def __str__(self):
        """ Print Class as string
        """
        return 'Queryspace ID: %s \nQuery: %s\nQuery ID: %s\n ' %(self.queryspace, self.querystring, self.queryident) 
    
        