'''
Created on Nov 4, 2017

@author: stelios
'''

class Schema(object):
    """Column class, equivalent to a Firethorn ADQL Column
    """


    def __init__(self, schema=None):
        self.__schema = schema
        
        
    def name(self):
        return self.__schema.name()
        
                        
    def __str__(self):
        """Get class as string
        """
        return self.__schema
