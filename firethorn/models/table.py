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


    def __init__(self, table=None):
        self.__table = table
        
        
    def name(self):
        return self.__table.name()   
    
            
    def as_astropy (self):
        """Get Astropy table
                             
        Returns
        -------
        astropy_table: Astropy.Table
            Table as Astropy table 
        """
        if (self.__table!=None):
            return astropy_Table.read(self.__table.url + "/votable", format="votable")        
        else:
            return None
                
    
    def rwocount (self):
        """Get Row count
        
        Returns
        -------
        rowcount: integer
            Count of rows  
        """  
        
        if (self.__table!=None):
            return self.__table.count()  
        else:
            return None        
          
    
                        
    def __str__(self):
        """Get class as string
        """
        return self.__table
