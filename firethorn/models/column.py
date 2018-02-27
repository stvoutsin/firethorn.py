'''
Created on Nov 4, 2017

@author: stelios
'''

class Column(object):
    """Column class, equivalent to a Firethorn ADQL Column
    """


    def __init__(self, column=None):
        self.__column = column
        
    
    def name(self):
        return self.__column.name()                    
    
    
    def __str__(self):
        """Get class as string
        """
        return self.__column
