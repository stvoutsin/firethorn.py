'''
Created on Nov 4, 2017

@author: stelios
'''

from astropy.table import Table as astropy_Table
import urllib.request
import logging
try:
    import simplejson as json
except ImportError:
    import json
from config import *
import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', category=AstropyWarning)

class Table(object):
    """Table class, equivalent to a Firethorn ADQL Table
    
    Attributes
    ----------
    tableident: string, optional
        The Identity URL of the table  
    """


    def __init__(self, firethorn_engine=None, table=None):
        
        self.table = table
        self.firethorn_engine = firethorn_engine
        if (self.table!=None):
            self.astropy_table = astropy_Table.read(self.table.url + "/votable", format="votable")        
        else:
            self.astropy_table = None
        
                   
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

        if self.table!=None:
            return self.table.get_error()
    
    
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
