'''
Created on Feb 8, 2018

@author: stelios
'''
from base.base_column import BaseColumn

class IvoaColumn(BaseColumn):
    """
    classdocs
    """


    def __init__(self, auth_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(auth_engine, json_object, url) 