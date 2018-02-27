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


    def __init__(self, account, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(account, json_object, url) 
        

    def select_schemas(self):
        
        schema_list = []
        json_list = self.get_json(self.url + "/schemas/select")
        
        for schema in json_list:
            schema_list.append(ivoa.IvoaSchema(json_object=schema, ivoa_resource=self))
        
        return schema_list
    
    
    def select_schema_by_ident(self, ident):
        return ivoa.IvoaSchema(url=ident,  ivoa_resource=self)
    
    
    def select_schema_by_name(self,schema_name):
        response_json = {}
        try :
            response_json = self.get_json(self.url + "/schemas/select", {config.ivoa_schema_select_by_name_param : schema_name })
        except Exception as e:
            logging.exception(e)      
            
        return ivoa.IvoaSchema(json_object = response_json, ivoa_resource=self)  
    

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
            if (self.account.password!=None and self.account.community!=None):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                              "firethorn.auth.password", self.account.password,
                                              "firethorn.auth.community",self.account.community
                                            ])
            elif (self.account.password!=None ):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                              "firethorn.auth.password", self.account.password,
                                            ])    
            elif (self.account.community!=None ):
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                              "firethorn.auth.community", self.account.community,
                                            ])    
            else:
                c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                            ])    
                                                   
            c.perform()
            c.close()
            buf.close() 
            
        except Exception as e:
            logging.exception(e)
     
        return self
    