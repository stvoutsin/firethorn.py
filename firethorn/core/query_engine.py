'''
Created on Jun 4, 2014

@author: stelios
'''

import urllib.request
import time
import signal
from models.adql_query.adql_query import AdqlQuery
try:
    import simplejson as json
except ImportError:
    import json
from utils.string_functions import string_functions
string_functions = string_functions()
import config as config
import logging


class Timeout():
    """Timeout class using ALARM signal."""
    class Timeout(Exception):
        pass

    def __init__(self, sec):
        self.sec = sec

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.raise_timeout)
        signal.alarm(self.sec)

    def __exit__(self, *args):
        signal.alarm(0)    # disable alarm

    def raise_timeout(self, *args):
        raise Timeout.Timeout()




class QueryEngine(object):
    """Query engine, used to drive queries through the core Firethorn service 
    """



    def __init__(self, auth_engine=None):
        self.id = None
        self.auth_engine = auth_engine
        
        
    def get_status(self, url):
        """Wrapper function to send a GET request to get the status of a resource and return the JSON
        
        Parameters
        ----------
        url: string, required
            URL to query
                   
        Returns    
        -------
        query_json: string
            The JSON response as a string
        
        """
 
        request = urllib.request.Request(url,headers=self.auth_engine.get_identity_as_headers())
        with urllib.request.urlopen(request) as response:
            query_json = response.read().decode('utf-8')
        return query_json            
            
            
    def _getRows(self, query_results):
        """Get rows from a query result

        
        Parameters
        ----------
        query_results: string, required
            Query results
                   
        Returns    
        -------
        row_length: integer
            The number of rows
        
        """
        rows = json.loads(query_results)
        row_length = -1
        if len(rows)<=1:
            return row_length
        else :
            row_length = len(rows[1])
 
        return row_length
    
    
                
    def run_query(self, query=None, query_name="", resource=None, query_mode="AUTO", test_email="", mode="SYNC", **kwargs):
        """
        Run a query on a resource
        """
        
        query_space=''
        
        if (resource!=None):
            query_space = string_functions.decode(resource.url)
            
        query_identity = ""


        def read_json(url):
            request = urllib.request.Request(url,headers=self.auth_engine.get_identity_as_headers())
            with urllib.request.urlopen(request) as response:
                query_json = response.read().decode('utf-8')
            return query_json        

        try :
            from datetime import datetime
            t = datetime.now()
            if query_name=="":
                query_name = 'query-' + t.strftime("%y%m%d_%H%M%S")
                     
            urlenc = { config.query_name_param : query_name,  config.query_param : query, config.query_mode_param : query_mode}
            data = urllib.parse.urlencode(urlenc).encode('utf-8')
            request = urllib.request.Request(query_space + config.query_create_uri, data,headers=self.auth_engine.get_identity_as_headers())
    
            with urllib.request.urlopen(request) as response:
                query_create_result = json.loads(response.read().decode('utf-8'))
            query_identity = query_create_result["self"]
            
            # Update query
            urlenc_updt = { config.query_limit_rows_param : config.firethorn_limits_rows_absolute, config.query_limit_time_param : config.firethorn_limits_time }
            data_updt = urllib.parse.urlencode(urlenc_updt).encode('utf-8')
            request_updt = urllib.request.Request(query_identity, data_updt,headers=self.auth_engine.get_identity_as_headers())
                            
            with urllib.request.urlopen(request_updt) as response:
                response.read().decode('utf-8')

            if mode.upper()=="SYNC":
                self.start_query_loop(query_identity)
            else:
                data = urllib.parse.urlencode({ config.query_status_update : "COMPLETED", "adql.query.wait.time" : 60000}).encode('utf-8')
                request = urllib.request.Request(query_identity, data,headers=self.auth_engine.get_identity_as_headers())            
                with urllib.request.urlopen(request) as response:
                    json.loads(response.read().decode('utf-8'))
                    
        except Exception as e:
            if (type(e).__name__=="Timeout"):
                raise
            else:
                logging.exception(e)

                    
        return AdqlQuery(json_object=query_create_result, auth_engine=self.auth_engine)
     
    
    def create_query(self, adql_query_input, adql_query_status_next, adql_resource, auth_engine, adql_query_wait_time=600000, jdbc_schema_ident=None):
        """
        Create query
        """
        
        json_result = {}
        
        try :
            from datetime import datetime                     
            urlenc = {}
            
            if (adql_query_input!=None):
                urlenc.update({config.query_param : adql_query_input})     
            if (adql_query_status_next!=None):
                urlenc.update({config.query_status_update : adql_query_status_next})
            if (adql_query_wait_time!=None):
                urlenc.update({config.query_wait_time_param : adql_query_wait_time})
            if (jdbc_schema_ident!=None):
                urlenc.update({config.jdbc_schema_ident : jdbc_schema_ident})   
            
            data = urllib.parse.urlencode(urlenc).encode('utf-8')
            request = urllib.request.Request(adql_resource.url + config.query_create_uri, headers=auth_engine.get_identity_as_headers())
            with urllib.request.urlopen(request, data) as response:
                json_result = json.loads(response.read().decode('UTF-8'))
                
        except Exception as e:
            if (type(e).__name__=="Timeout"):
                raise
            else:
                logging.exception(e)

        return AdqlQuery(json_object=json_result, auth_engine=auth_engine)


    def update_query(self,  adql_query, auth_engine, adql_query_input=None, adql_query_status_next=None, adql_query_wait_time=None):
        """
        Create query
        """
        
        json_result = {}
        
        try :
            
            urlenc = {}
            
            if (adql_query_input!=None):
                urlenc.update({config.query_param : adql_query_input})           
            if (adql_query_status_next!=None):
                urlenc.update({config.query_status_update : adql_query_status_next})
            if (adql_query_wait_time!=None):
                urlenc.update({config.query_wait_time_param : adql_query_wait_time})
                                                
            data = urllib.parse.urlencode(urlenc).encode('utf-8')
            request = urllib.request.Request(adql_query.url, headers=auth_engine.get_identity_as_headers())

            with urllib.request.urlopen(request, data) as response:
                json_result = json.loads(response.read().decode('UTF-8'))
                
        except Exception as e:
            if (type(e).__name__=="Timeout"):
                raise
            else:
                logging.exception(e)

        return AdqlQuery(json_object=json_result, auth_engine=auth_engine)


    
    def start_query_loop(self, url, test_email=""):
        """
        Start the query loop
        
        @param url: A URL string to be used
        @return: Results of query
        """
        
        delay = config.INITIAL_DELAY
        start_time = time.time()
        elapsed_time = 0
        query_json = {'syntax' : {'friendly' : 'A problem occurred while running your query', 'status' : 'Error' }}
        
        try:
    
            data = urllib.parse.urlencode({ config.query_status_update : "COMPLETED", "adql.query.wait.time" : 60000}).encode('utf-8')
            request = urllib.request.Request(url, data,headers=self.auth_engine.get_identity_as_headers())
            
            with urllib.request.urlopen(request) as response:
                query_json =  json.loads(response.read().decode('utf-8'))
            query_status = "QUEUED"

            while query_status=="QUEUED" or query_status=="RUNNING" and elapsed_time<config.MAX_ELAPSED_TIME:
                query_json = json.loads(self.get_status(url))
                query_status= query_json["status"]
                time.sleep(delay)
                if elapsed_time>config.MIN_ELAPSED_TIME_BEFORE_REDUCE and delay<config.MAX_DELAY:
                    delay = delay + delay
                elapsed_time = int(time.time() - start_time)

          
            if query_status=="ERROR" or query_status=="FAILED":
                if (query_json["syntax"]["status"]=="PARSE_ERROR"):
                    return {'Code' :-1,  'Content' : 'Query error: ' + query_json["syntax"]["status"] + ' - ' + query_json["syntax"]["friendly"] }
                else:
                    return {'Code' :-1,  'Content' : 'Query error: A problem occurred while running your query'}
            elif query_status=="CANCELLED":
                return {'Code' :1,  'Content' : 'Query error: Query has been canceled' }
            elif query_status=="EDITING":
                return {'Code' :-1,  'Content' :  query_json["syntax"]["status"] + ' - ' + query_json["syntax"]["friendly"] }
            elif query_status=="COMPLETED":
                return {'Code' :1,  'Content' : query_json["results"]["formats"]["datatable"] }
            elif elapsed_time>=config.MAX_ELAPSED_TIME:
                return {'Code' :-1,  'Content' : 'Query error: Max run time (' + str(config.MAX_ELAPSED_TIME) + ' seconds) exceeded' }
            else:
                return {'Code' :-1,  'Content' : 'Query error: A problem occurred while running your query' }
            
        
        except Exception as e:
            if (type(e).__name__=="Timeout"):
                raise
            else:
                logging.exception(e)
            return {'Code' :-1,  'Content' : 'Query error: A problem occurred while running your query' }

    
    
          
            
