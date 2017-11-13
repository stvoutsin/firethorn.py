'''
Created on Nov 4, 2017

@author: stelios
'''
import logging
from models.table import Table
from core.query_engine import QueryEngine
import urllib2
try:
    import simplejson as json
except ImportError:
    import json
from config import firethorn_config as config


class Query(object):
    '''
    Query class
    '''

    def __init__(self, querystring=None, queryspace=None, queryident=None):
        '''
        Constructor
        
        Parameters
        ----------
        '''
        self.table = Table()
        self.error = None
        self.querystring = querystring
        self.queryspace = queryspace
        self.firethorn_query_engine = QueryEngine()
        self.queryident = queryident
       
       
    def __get_json(self, url): 
        query_json=[]
        try:
            request = urllib2.Request(url, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"})
            f_read = urllib2.urlopen(request)
            query_json = json.loads(f_read.read())
            f_read.close()
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
        
                     
    def run (self, mode="SYNC"):
        '''
        Run Query
        '''
        try: 
            self.queryident = self.firethorn_query_engine.run_query(self.querystring, "", self.queryspace, "AUTO", config.test_email, mode)
        except Exception as e:
            logging.exception(e)    
        return 
    

    def results (self):
        '''
        Get Results
        '''
        try: 
            if not self.table.tableident:
                self.table = Table(self.__get_json(self.queryident).get("results",[]).get("table",None))
                print (self.__get_json(self.queryident))
        except Exception as e:
            logging.exception(e)    

        return self.table



    def status (self):
        '''
        Get Status message for query
        '''
        
        status = "UNKNOWN"
        
        try:
            statusjson = self.firethorn_query_engine.get_status(self.queryident)
            status = json.loads(statusjson).get("status","UNKNOWN")
        except Exception as e:
            logging.exception(e)
        
        return status
                   
                   
    def get_error (self):
        '''
        Get Error message
        '''
        try: 
            if not self.error:
                self.error = self.__get_json(self.queryident).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error

                        
    def __str__(self):
        return 'Queryspace ID: %s \nQuery: %s\nQuery ID: %s\n ' %(self.queryspace, self.querystring, self.queryident) 
    
    