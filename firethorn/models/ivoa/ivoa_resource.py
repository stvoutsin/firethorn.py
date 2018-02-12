'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    from base.base_resource import BaseResource
    import ivoa
    import urllib
    import json
    import config as config
    import logging
    import pycurl
    import io
    import uuid
except Exception as e:
    logging.exception(e)


class IvoaResource(BaseResource):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        

    def select_schemas(self):
        return self.firethorn_engine.get_json(self.url + "/schemas/select")
    
    
    def select_schema_by_ident(self, ident):
        return ivoa.IvoaSchema(url=ident, firethorn_engine=self.firethorn_engine)
    
    
    def select_schema_by_name(self,schema_name):
        response_json = {}
        try :
            data = urllib.parse.urlencode({config.ivoa_schema_select_by_name_param : schema_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/schemas/select", headers=self.firethorn_engine.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return ivoa.IvoaSchema(json_object = response_json, firethorn_engine=self.firethorn_engine)  
    

    def import_ivoa_metadoc(self, metadoc):
        """Import IVOA metadoc
        
        Parameters
        ----------
        metadoc: string, required
            Metadoc location

            
        Returns
        -------
        self: IvoaResource
            Copy of this IVOA resource
        
        """
      

        buf = io.BytesIO()
        vosi_file = None
        
        try:
           
            c = pycurl.Curl()   
            if (metadoc.lower().startswith("http://") or metadoc.lower().startswith("https://")):
                unique_filename = str(uuid.uuid4())
                tmpname = "/tmp/" + unique_filename

                with urllib.request.urlopen(metadoc) as response, open(tmpname, 'wb') as out_file:
                    data = response.read() # a `bytes` object
                    out_file.write(data)
                
                vosi_file = tmpname

            url = self.url + "/vosi/import"        
            values = [  
                      ("vosi.tableset", (c.FORM_FILE, vosi_file ))]
                       
            c.setopt(c.URL, str(url))
            c.setopt(c.HTTPPOST, values)
            c.setopt(c.WRITEFUNCTION, buf.write)
            if (self.firethorn_engine.identity.password!=None and self.firethorn_engine.identity.community!=None):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                              "firethorn.auth.password", self.firethorn_engine.identity.password,
                                              "firethorn.auth.community",self.firethorn_engine.identity.community
                                            ])
            elif (self.firethorn_engine.identity.password!=None ):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                              "firethorn.auth.password", self.firethorn_engine.identity.password,
                                            ])    
            elif (self.firethorn_engine.identity.community!=None ):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                              "firethorn.auth.community", self.firethorn_engine.identity.community,
                                            ])    
            else:
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                            ])    
                                                   
            c.perform()
            c.close()
            buf.close() 
            
        except Exception as e:
            logging.exception(e)
     
        return self
    