'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    from base.base_schema import BaseSchema
    import ivoa
    import json
    import config as config
    import logging
    import urllib.request
except Exception as e:
    logging.exception(e)

class IvoaSchema(BaseSchema):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor
        """
        super().__init__(firethorn_engine, json_object, url) 
        

    def select_tables(self):
        return self.firethorn_engine.get_json(self.url + "/tables/select")
    
    
    def select_table_by_ident(self, ident):
        return ivoa.IvoaTable(url=ident, firethorn_engine=self.firethorn_engine)
    
    
    def select_table_by_name(self,table_name):
        response_json = {}
        try :
            data = urllib.parse.urlencode({config.ivoa_table_select_by_name_param : table_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/tables/select", headers=self.firethorn_engine.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return ivoa.IvoaTable(json_object = response_json, firethorn_engine=self.firethorn_engine)  
            
            