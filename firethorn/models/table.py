'''
Created on Nov 4, 2017

@author: stelios
'''

from astropy.table import Table as astropy_Table
from cStringIO import StringIO
import urllib2
import logging
try:
    import simplejson as json
except ImportError:
    import json
from config import firethorn_config as config


class Table(object):
    '''
    Table class
    '''


    def __init__(self, tableident=None):
        '''
        Constructor
        
        Parameters
        ----------
    
        '''
        self.tableident = tableident
        if (self.tableident):
            self.astropy_table = astropy_Table.read(StringIO(self.__get_votable(tableident + "/votable")), format="votable")
        else:
            self.astropy_table = None
        
        self.error = None
        
    
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
             
             
    def __get_votable(self, url): 
        query_xml=[]
        try:
            request = urllib2.Request(url, headers={"firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"})
            f_read = urllib2.urlopen(request)
            query_xml = f_read.read()
            f_read.close()
        except Exception as e:
            logging.exception(e)
        return query_xml  
        
                   
    @property
    def astropy_table(self):
        return self.__astropy_table
        
        
    @astropy_table.setter
    def astropy_table(self, astropy_table):
        self.__astropy_table = astropy_table
 
 
    @property
    def tableident(self):
        return self.__tableident
        
        
    @tableident.setter
    def tableident(self, tableident):
        self.__tableident = tableident       
 
        
    def as_astropy (self):
        '''
        Get Astropy table 
        '''
        return self.astropy_table
                
                
    def get_error (self):
        '''
        Get Error message
        '''
        try: 
            if not self.error:
                self.error = self.__get_json(self.tableident).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error
    
    
    def rwocount (self):
        '''
        Get Error message
        '''
        rowcount=None
        try: 
            rowcount = self.__get_json(self.tableident).get("metadata",[]).get("adql",[]).get("count",None)
        except Exception as e:
            logging.exception(e) 
               
        return rowcount    
    
                        
    def __str__(self):
        return 'Table ID: %s\n ' %(self.tableident) 
