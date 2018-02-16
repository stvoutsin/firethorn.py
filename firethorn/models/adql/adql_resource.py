'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    from base.base_resource import BaseResource
    from adql_query import adql_query
    import adql
    import urllib
    import json
    import config as config
    import logging
    import pycurl
    import io
    import uuid
    import urllib.request
    from core.query_engine import QueryEngine
except Exception as e:
    logging.exception(e)
    
    
class AdqlResource(BaseResource):
    """
    classdocs
    """


    def __init__(self, firethorn_engine, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(firethorn_engine, json_object, url) 
        
    
    def create_adql_schema(self, schema_name):
        """
        Create an Adql schema
 
        """

        try:
           
            if schema_name!="":
                data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-schema-1.0.json'] : schema_name}).encode("utf-8")
                req = urllib.request.Request( self.url + config.schema_create_uri, headers=self.firethorn_engine.identity.get_identity_as_headers()) 
                with urllib.request.urlopen(req, data) as response:
                    json_result = json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logging.exception(e)
            
        return adql.AdqlSchema(firethorn_engine = self.firethorn_engine, json_object = json_result)
    
    
    def import_ivoa_schema(self, ivoa_schema, schema_name=None):

        try:
            if schema_name:
                importname = schema_name
            else:
                importname = ivoa_schema.name()
                
            if importname!="":
                data = urllib.parse.urlencode({config.workspace_import_schema_base : ivoa_schema.url, config.workspace_import_schema_name : importname}).encode("utf-8")
                req = urllib.request.Request( self.url + config.workspace_import_uri, headers=self.firethorn_engine.identity.get_identity_as_headers()) 
                with urllib.request.urlopen(req, data) as response:
                    json_result = json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logging.exception(e)
            
        return adql.AdqlSchema(firethorn_engine = self.firethorn_engine, json_object = json_result)
    
    
    def import_jdbc_schema(self, jdbc_schema, schema_name=None, metadoc=None):                  
        """Import a JDBC Schema
        
        Parameters
        ----------
        JdbcSchema: string, required
            JdbcSchema to import
            
        schema_name: string, required
            JDBC Schema name
            
        metadoc: string, optional
            Metadocfile location string 
            
        
        Returns    
        -------
        adqlschema: string
            The AdqlSchema created
        
        """
        
        ## Needs refactoring to not use temp file
      
        adqlschema={}
        
        if (metadoc!=None):
            #buf = StringIO()
            buf = io.BytesIO()
            try:
               
                c = pycurl.Curl()   
    
                if (metadoc.lower().startswith("http://") or metadoc.lower().startswith("https://")):
                    unique_filename = str(uuid.uuid4())
                    tmpname = "/tmp/" + unique_filename
    
                    with urllib.request.urlopen(metadoc) as response, open(tmpname, 'wb') as out_file:
                        data = response.read() # a `bytes` object
                        out_file.write(data)
                    
                    metadocfile = tmpname
    
                c = pycurl.Curl()    
                url = self.url + "/metadoc/import"        
                values = [  
                          ("metadoc.base", str(jdbc_schema.url)),
                          ("metadoc.file", (c.FORM_FILE, metadocfile))]
     
                c.setopt(c.URL, str(url))
                c.setopt(c.HTTPPOST, values)
                c.setopt(c.WRITEFUNCTION, buf.write)
                if (self.firethorn_engine.identity.password!=None and self.firethorn_engine.identity.community!=None):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                                  "firethorn.auth.password", self.firethorn_engine.identity.password,
                                                  "firethorn.auth.community",self.firethorn_engine.identity.community
                                                ])
                elif (self.identity.password!=None ):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                                  "firethorn.auth.password", self.firethorn_engine.identity.password,
                                                ])    
                elif (self.identity.community!=None ):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                                  "firethorn.auth.community", self.firethorn_engine.identity.community,
                                                ])    
                else:
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.firethorn_engine.identity.username,
                                                ])    
                         
                c.perform()
                c.close()
                adqlschema = json.loads(buf.getvalue().decode("utf-8"))[0]
                buf.close() 
                
            except Exception as e:
                logging.exception(e)
        else :
    
            try:
                importname = schema_name
                if importname!="":
                    data = urllib.parse.urlencode({config.workspace_import_schema_base : jdbc_schema.url, config.workspace_import_schema_name : importname}).encode("utf-8")
                    req = urllib.request.Request( self.url + config.workspace_import_uri, headers=self.firethorn_engine.identity.get_identity_as_headers()) 
                    with urllib.request.urlopen(req, data) as response:
                        adqlschema = json.loads(response.read().decode("utf-8"))
                        
            except Exception as e:
                logging.exception(e)
    
        return adql.AdqlSchema(firethorn_engine = self.firethorn_engine, json_object = adqlschema)
    
        
    def import_adql_schema(self, adql_schema, schema_name=None):                   
        """Import a schema into a workspace for querying
        
        Parameters
        ----------
        adql_schema: AdqlSchema, required
            The AdqlSchema to import

        schema_name: string, optional
            Name of Schema
        
        """
        json_result = {}
        try:
            if schema_name:
                importname = schema_name
            else:
                importname = adql_schema.name()
            if importname!="":
                data = urllib.parse.urlencode({config.workspace_import_schema_base : adql_schema.url, config.workspace_import_schema_name : importname}).encode("utf-8")
                req = urllib.request.Request( self.url + config.workspace_import_uri, headers=self.firethorn_engine.identity.get_identity_as_headers()) 
                with urllib.request.urlopen(req, data) as response:
                    json_result = json.loads(response.read().decode("utf-8"))
        except Exception as e:
            logging.exception(e)
            
        return adql.AdqlSchema(firethorn_engine = self.firethorn_engine, json_object = json_result)
    
    
    def select_schemas(self):
        return self.firethorn_engine.get_json(self.url + "/schemas/select")
    
    
    def select_schema_by_ident(self, ident):
        return adql.AdqlSchema(url=ident, firethorn_engine=self.firethorn_engine)
    
    
    def select_schema_by_name(self,schema_name):
        response_json = {}
        try :
            data = urllib.parse.urlencode({config.resource_create_name_params["http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-schema-1.0.json"] : schema_name }).encode("utf-8")
            req = urllib.request.Request( self.url + "/schemas/select", headers=self.firethorn_engine.identity.get_identity_as_headers())
            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return adql.AdqlSchema(json_object = response_json, firethorn_engine=self.firethorn_engine) 
    

    def create_query(self, adql_query_input, adql_query_status_next="COMPLETED", adql_query_wait_time=600000):
        qry_engine = QueryEngine()
        return qry_engine.create_query(adql_query_input=adql_query_input, adql_query_status_next=adql_query_status_next, adql_resource=self, firethorn_engine=self.firethorn_engine, adql_query_wait_time=adql_query_wait_time)
    
    
    def select_queries(self):
        response_json = {}
        try :

            req = urllib.request.Request( self.url + "/queries/select", headers=self.firethorn_engine.identity.get_identity_as_headers())
            with urllib.request.urlopen(req) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return response_json
        
    
    