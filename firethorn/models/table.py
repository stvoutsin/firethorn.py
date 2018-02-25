'''
Created on Nov 4, 2017

@author: stelios
'''

from astropy.table import Table as astropy_Table
import logging
try:
    import simplejson as json
except ImportError:
    import json
import warnings
from astropy.utils.exceptions import AstropyWarning
warnings.simplefilter('ignore', category=AstropyWarning)

class Table(object):
    """Table class, equivalent to a Firethorn ADQL Table
    """


    def __init__(self, auth_engine=None, table=None):
        
        self.table = table
        self.auth_engine = auth_engine
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
        
        
    def as_astropy (self):
        """Get Astropy table
                    response_json = self.get_json(self.url + "/tables/select", { "jdbc.table.name": table_name })                

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
            rowcount = self.get_json(self.table.url).get("metadata",[]).get("adql",[]).get("count",None)
        except Exception as e:
            logging.exception(e) 
               
        return rowcount    
    
                        
    def __str__(self):
        """Get class as string
        """
        return self.astropy_table
