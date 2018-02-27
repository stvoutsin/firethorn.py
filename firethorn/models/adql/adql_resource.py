'''
Created on Feb 8, 2018

@author: stelios
'''

try:
    from base.base_resource import BaseResource
    import models
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


    def __init__(self, account, json_object=None, url=None):
        """
        Constructor    
        """
        super().__init__(account, json_object, url) 
        
    
    def create_adql_schema(self, schema_name):
        """
        Create an Adql schema
 
        """ 
      
        response_json = {}
        
        try:
            if schema_name!="":
                response_json = self.get_json(self.url + config.schema_create_uri, {config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-schema-1.0.json'] : schema_name})

        except Exception as e:
            logging.exception(e)
            
        return models.adql.AdqlSchema(adql_resource = self, json_object = response_json)
    
    
    def import_ivoa_schema(self, ivoa_schema, schema_name=None):
        response_json = {}

        try:
            if schema_name:
                importname = schema_name
            else:
                importname = ivoa_schema.name()
                
            if importname!="":
                response_json = self.get_json( self.url + config.workspace_import_uri, {config.workspace_import_schema_base : ivoa_schema.url, config.workspace_import_schema_name : importname})

        except Exception as e:
            logging.exception(e)
            
        return models.adql.AdqlSchema(adql_resource = self, json_object = response_json)
    
    
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
      
        response_json={}
        
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
                if (self.account.password!=None and self.account.community!=None):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                                  "firethorn.auth.password", self.account.password,
                                                  "firethorn.auth.community",self.account.community
                                                ])
                elif (self.identity.password!=None ):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                                  "firethorn.auth.password", self.account.password,
                                                ])    
                elif (self.identity.community!=None ):
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                                  "firethorn.auth.community", self.account.community,
                                                ])    
                else:
                    c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.username", self.account.username,
                                                ])    
                         
                c.perform()
                c.close()
                response_json = json.loads(buf.getvalue().decode("utf-8"))[0]
                buf.close() 
                
            except Exception as e:
                logging.exception(e)
        else :
    
            try:
                if schema_name!=None:
                    importname = schema_name
                else :
                    importname = jdbc_schema.name()
                    
                if importname!="":
                    response_json = self.get_json( self.url + config.workspace_import_uri, {config.workspace_import_schema_base : jdbc_schema.url, config.workspace_import_schema_name : importname})
                        
            except Exception as e:
                logging.exception(e)
    
        return models.adql.AdqlSchema(adql_resource = self, json_object = response_json)
    
        
    def import_adql_schema(self, adql_schema, schema_name=None):                   
        """Import a schema into a workspace for querying
        
        Parameters
        ----------
        adql_schema: AdqlSchema, required
            The AdqlSchema to import

        schema_name: string, optional
            Name of Schema
        
        """
        
        response_json = {}
        importname=""
        
        try:
            if schema_name:
                importname = schema_name
            else:
                importname = adql_schema.name()
                
            print (importname)
            if importname!="":
                response_json = self.get_json( self.url + config.workspace_import_uri, {config.workspace_import_schema_base : adql_schema.url, config.workspace_import_schema_name : importname})

        except Exception as e:
            logging.exception(e)
            
        return models.adql.AdqlSchema(adql_resource = self, json_object = response_json)
    
    
    def select_schemas(self):
        """
        Select Schemas, returns a list of AdqlSchema objects
        """
        schema_list = []
        json_list = self.get_json(self.url + "/schemas/select")
        for schema in json_list:
            schema_list.append(models.adql.AdqlSchema(json_object=schema, adql_resource = self))
        return schema_list
    
    
    def select_schema_by_ident(self, ident):
        """
        Select by identity, returns an AdqlSchema object
        """
        return models.adql.AdqlSchema(url=ident, adql_resource = self)
    
    
    def select_schema_by_name(self,schema_name):
        """
        Select Schema by name, returns an AdqlSchema object
        """
        response_json = self.get_json( self.url + "/schemas/select", {config.resource_create_name_params["http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-schema-1.0.json"] : schema_name })
        return models.adql.AdqlSchema(json_object = response_json, adql_resource=self) 
    

    def create_query(self, adql_query_input, adql_query_status_next=None, jdbc_schema_ident=None, adql_query_wait_time=600000):
        """
        Create a query on this resource
        """
        qry_engine = QueryEngine()
        return qry_engine.create_query(adql_query_input=adql_query_input, adql_query_status_next=adql_query_status_next, adql_resource=self, account=self.account, adql_query_wait_time=adql_query_wait_time, jdbc_schema_ident=jdbc_schema_ident)
    
    
    def select_queries(self):
        """
        Select queries for this resource, returns a list of AdqlQuery objects
        """
        query_list = []

        response_json = self.get_json( self.url + "/queries/select")
        for query in response_json:
            query_list.append(models.adql_query.AdqlQuery(json_object=query, adql_resource=self))
            
        return query_list
        
    
    