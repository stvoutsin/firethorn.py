'''
Created on Nov 4, 2017

@author: stelios
'''

class Column(object):
    """Column class, equivalent to a Firethorn ADQL Column
    """


    def __init__(self, adql_column=None):
        self.__adql_column = adql_column
        
    
    def name(self):
        return self.__adql_column.name()                    
    
    
    def __str__(self):
        """Get class as string
        """
        return self.__adql_column
