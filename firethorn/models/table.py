'''
Created on Nov 4, 2017

@author: stelios
'''

from astropy.table import Table as astropy_Table
from io import StringIO
import urllib.request
import logging
try:
    import simplejson as json
except ImportError:
    import json
from config import firethorn_config as config


class Table(object):
    """Table class, equivalent to a Firethorn ADQL Table
    
    Attributes
    ----------
    tableident: string, optional
        The Identity URL of the table  
    """


    def __init__(self, tableident=None):
        self.tableident = tableident
        if (self.tableident):
            self.astropy_table = astropy_Table.read(tableident + "/votable", format="votable")        
        else:
            self.astropy_table = None
        
        self.error = None
        
    
    def __get_json(self, url): 
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

        query_json=[]
        try:
            request = urllib.request.Request(url, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"})
            with urllib.request.urlopen(request) as response:
                query_json = json.loads(response.read().decode('ascii'))            
        except Exception as e:
            logging.exception(e)
        return query_json    
             
             
    def __get_votable(self, url): 
        """Get request to a service that returns a VOTable
        
        Parameters
        ----------
        url: string, required
            VOTable web Resource URL
            
        Returns    
        -------
        query_xml: string
            XML as string returned by GET request
        
        """

        query_xml=""
        request=None
        try:
            request = urllib.request.Request(url, headers={"firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"})
            with urllib.request.urlopen(request) as response:
                query_xml =  response.read().decode('ascii')   
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
        """Get Astropy table
        
        Returns
        -------
        astropy_table: Astropy.Table
            Table as Astropy table 
        """
        return self.astropy_table
                
                
    def get_error (self):
        """Get Error message
        
        Returns
        -------
        self.error: string
            Error Message 
        """        
        try: 
            if not self.error:
                self.error = self.__get_json(self.tableident).get("syntax",[]).get("friendly",None)
        except Exception as e:
            logging.exception(e) 
               
        return self.error
    
    
    def rwocount (self):
        """Get Row count
        
        Returns
        -------
        rowcount: integer
            Count of rows  
        """  
        rowcount=None
        try: 
            rowcount = self.__get_json(self.tableident).get("metadata",[]).get("adql",[]).get("count",None)
        except Exception as e:
            logging.exception(e) 
               
        return rowcount    
    
                        
    def __str__(self):
        """Get class as string
        """
        return 'Table ID: %s\n ' %(self.tableident) 
