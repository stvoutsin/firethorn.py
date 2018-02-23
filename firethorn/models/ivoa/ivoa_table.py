'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    from base.base_table import BaseTable
    import ivoa
    import json
    import config as config
    import logging
    import urllib.request
except Exception as e:
    logging.exception(e)


class IvoaTable(BaseTable):
    """
    classdocs
    """


    def __init__(self, auth_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(auth_engine, json_object, url) 
    
    
    def select_columns(self):
        return self.get_json(self.url + "/columns/select")
    
        
    def select_column_by_ident(self, ident):
        return ivoa.IvoaColumn(url=ident, auth_engine=self.auth_engine)
    
    
    def select_column_by_name(self,column_name):
        response_json = {}
        try :
            data = urllib.parse.urlencode({config.ivoa_column_select_by_name_param : column_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/columns/select", headers=self.auth_engine.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return ivoa.IvoaColumn(json_object = response_json, auth_engine=self.auth_engine)  
    