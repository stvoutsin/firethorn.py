'''
Created on Jul 22, 2013

@author: stelios
'''

import urllib
import json
import logging
from datetime import datetime
import config.firethorn_config as config
from io import StringIO




class FirethornEngine(object):
    """ Provides the low level methods to setup Firethorn services, including JDBC connections and importing IVOA or local resources
    
    Attributes
    ----------
    jdbcspace: string, optional
        JDBC URL Endpoint for the Firethorn service
        
    adqlspace: string, optional
        ADQL Resource URL Endpoint for the Firethorn service

    adqlschema: string, optional
        ADQL Schema URL to initialize with

    query_schema: string, optional 
         Query schema URL for the Firethorn resource
   
    schema_name: string, optional
        Name of the ADQL Schema URL to initialize with

    schema_alias: string, optional
        Alias of the ADQL Schema URL to initialize with
        
    driver: string, optional
        Driver used in Firethorn JDBC Connections    

    endpoint: string, optional
        Endpoint used for the Firethorn services    

    """


    def __init__(self, jdbcspace="", adqlspace="", adqlschema="", query_schema="", schema_name="", schema_alias="", driver="", endpoint = "" , **kwargs):

        self.jdbcspace = ""
        self.adqlspace =  ""
        self.adqlschema = ""
        self.query_schema = ""
        self.schema_name = ""
        self.schema_alias = ""
        self.jdbcspace = jdbcspace
        self.adqlspace =  adqlspace
        self.adqlschema = adqlschema
        self.query_schema = query_schema
        self.schema_name = schema_name
        self.schema_alias = schema_alias
        self.driver = driver
        self.endpoint = endpoint
        
        
    def setup_firethorn_environment(self, resourcename ,resourceuri, catalogname, ogsadainame, adqlspacename, jdbccatalogname, jdbcschemaname, metadocfile, jdbc_resource_user="", jdbc_resource_pass=""):
        """Initialise the Firethorn environment
        Import metadata, setup initial workspace, import schemas, tables
        
        Parameters
        ----------
        resourcename: string, required
            Resource name
            
        resourceuri: string, required
            Resource URI
            
        catalogname: string, required
            Catalog name
            
        ogsadainame: string, required
            OGSADAI name
            
        adqlspacename: string, required
            ADQL Space name
            
        jdbccatalogname: string, required
            JDBC Catalog name
            
        jdbcschemaname: string, required
            JDBC Schema name
            
        metadocfile:  string, required
            Metadocfile
            
        jdbc_resource_user: string, optional
            JDBC resource username
            
        jdbc_resource_pass: string, optional
            JDBC resource password
            
        """

        

        try:

            self.initialise_metadata_import(resourcename ,resourceuri, catalogname, ogsadainame, adqlspacename, jdbccatalogname, jdbcschemaname, metadocfile, jdbc_resource_user, jdbc_resource_pass)
            self.schema_name = self.get_attribute(self.adqlschema, "fullname" )
            self.schema_alias = self.get_attribute(self.adqlschema, "name" )
            self.query_schema = self.create_initial_workspace(self.schema_name, self.schema_alias, self.adqlschema)
        except Exception as e:
            logging.exception(e)

                    
    def initialise_metadata_import(self, resourcename ,resourceuri, catalogname, ogsadainame, adqlspacename, jdbccatalogname, jdbcschemaname, metadocfile, jdbc_resource_user="", jdbc_resource_pass="" ):
        """Import metadata, fetch Schema from file provided
        
        Parameters
        ----------
        resourcename: string, required
            Resource name
            
        resourceuri: string, required
            Resource URI
            
        catalogname: string, required
            Catalog name
            
        ogsadainame: string, required
            OGSADAI name
            
        adqlspacename: string, required
            ADQL Space name
            
        jdbccatalogname: string, required
            JDBC Catalog name
            
        jdbcschemaname: string, required
            JDBC Schema name
            
        metadocfile:  string, required
            Metadocfile
            
        jdbc_resource_user: string, optional
            JDBC resource username
            
        jdbc_resource_pass: string, optional
            JDBC resource password
            
        """

        self.jdbcspace = self.create_jdbc_space(resourcename ,resourceuri, catalogname, ogsadainame, jdbc_resource_user, jdbc_resource_pass)
        if (self.adqlspace=="" or self.adqlspace==None):
            self.adqlspace = self.create_adql_space(adqlspacename)
        self.adqlschema = self.import_jdbc_metadoc(self.adqlspace, self.jdbcspace, jdbccatalogname, jdbcschemaname, metadocfile)
         
         
    def create_jdbc_space(self, resourcename ,resourceuri, catalogname, ogsadainame, jdbc_resource_user="", jdbc_resource_pass=""):
        """Import metadata, fetch Schema from file provided
        
        Parameters
        ----------
        resourcename: string, required
            Resource name
            
        resourceuri: string, required
            Resource URI
            
        catalogname: string, required
            Catalog name
            
        ogsadainame: string, required
            OGSADAI name
      
        jdbc_resource_user: string, optional
            JDBC resource username
            
        jdbc_resource_pass: string, optional
            JDBC resource password
        
        
        Returns    
        -------
        jdbcspace: string
            The URL of the created JDBC resource
        
        """
        
        jdbcspace=""
        try:
         
            if jdbc_resource_user!="" and jdbc_resource_pass!="":
                data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/jdbc-resource-1.0.json'] : resourcename,
                                     "jdbc.connection.url" : resourceuri,	
                                     "jdbc.resource.catalog" : catalogname,
                                     "jdbc.resource.name" : ogsadainame,
                                     "jdbc.connection.driver" : self.driver,
                                     "jdbc.connection.user" : jdbc_resource_user,
                                     "jdbc.connection.pass" : jdbc_resource_pass
                                    }).encode("utf-8")
    

            else :
                data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/jdbc-resource-1.0.json'] : resourcename ,
                                     "jdbc.connection.url" : resourceuri,
                                     "jdbc.catalog.name" : catalogname,
				     "jdbc.connection.driver" : self.driver,

                                    }).encode("utf-8")



            req = urllib.request.Request( self.endpoint + config.jdbc_creator, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email , "firethorn.auth.community" : "public (unknown)"})
            response = urllib.request.urlopen(req,data)
            jdbcspace = json.loads(response.read())["ident"]
            response.close()
        except Exception as e:
            logging.exception(e)
        return jdbcspace    
    

    def import_jdbc_metadoc(self, adqlspace="", jdbcspace="", jdbccatalogname='', jdbcschemaname='dbo',metadocfile=""):
        """Import a JDBC Metadoc
        
        Parameters
        ----------
        adqlspace: string, required
            ADQL Resource URL
            
        jdbcspace: string, required
            JDBC Resource URL
            
        jdbccatalogname: string, required
            JDBC Catalog name
            
        jdbcschemaname: string, required
            JDBC schema name (defaults to 'dbo')
      
        metadocfile: string, optional
            Metadocfile location string 
            
        
        Returns    
        -------
        adqlschema: string
            The URL of the created ADQL Schema
        
        """
      
        jdbcschemaident = ""
        adqlschema=""
        import pycurl
        
        buf = StringIO.StringIO()
        try:
            data = urllib.parse.urlencode({"jdbc.schema.catalog" : jdbccatalogname,
                                     "jdbc.schema.schema" : jdbcschemaname}).encode("utf-8")
            req = urllib.request.Request( jdbcspace + "/schemas/select", headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email , "firethorn.auth.community" : "public (unknown)"})
            #response = urllib.request.urlopen(req,data)
            with urllib.request.urlopen(req, data) as response:
                js =  json.loads(response.read().decode('ascii'))
            
            jdbcschemaident = json.loads(js)["ident"]
            response.close()
        except Exception as e:
            logging.exception(e)
    
        try:
           
            c = pycurl.Curl()   
            
            url = adqlspace + "/metadoc/import"        
            values = [  
                      ("metadoc.base", str(jdbcschemaident)),
                      ("metadoc.file", (c.FORM_FILE, metadocfile))]
                       
            c.setopt(c.URL, str(url))
            c.setopt(c.HTTPPOST, values)
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.identity",config.test_email,
                                          "firethorn.auth.community","public (unknown)"
                                          ])
            c.perform()
            c.close()
            adqlschema = json.loads(buf.getvalue())[0]["ident"]
            buf.close() 
            
        except Exception as e:
            logging.exception(e)
     
        return adqlschema
    
    
    def create_adql_space(self, adqlspacename=None):
        """Create an ADQL Resource
        
        Parameters
        ----------
        adqlspacename: string, required
            ADQL Space name 
            
        
        Returns    
        -------
        adqlspace: string
            The URL of the created ADQL Resource
        
        """
        adqlspace = ""
        try:
            ### Create workspace
            if adqlspacename==None:
                t = datetime.now()
                adqlspacename = 'workspace-' + t.strftime("%y%m%d_%H%M%S") 
            data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-resource-1.0.json'] : adqlspacename}).encode("utf-8")
            req = urllib.request.Request( self.endpoint + config.workspace_creator, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email , "firethorn.auth.community" : "public (unknown)"})
            with urllib.request.urlopen(req, data) as response:
                adqlspace =  json.loads(response.read().decode('ascii'))["ident"]
            response.close()
        except Exception as e:
            logging.exception(e)
            
        return adqlspace
                         
                         
    def create_query_schema(self, resource=""):
        """Create a query schema
        
        Parameters
        ----------
        resource: string, required
            ADQL Resource URL
        
        Returns    
        -------
        query_schema: string
            The URL of the created Query Schema
        
        """
        query_schema = ""
        try:    
            ### Create Query Schema 
            data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-schema-1.0.json'] : "query_schema"}).encode("utf-8")
            req = urllib.request.Request( resource +  config.schema_create_uri, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"})
            with urllib.request.urlopen(req, data) as response:
                query_schema =  json.loads(response.read().decode('ascii'))["ident"]
            response.close()
        except Exception as e:
            logging.exception(e)
            
        return query_schema
    
        
    def create_initial_workspace(self, initial_catalogue_fullname, initial_catalogue_alias, initial_catalogue_ident):

        """Create the inital workspace given a name, alias and catalogue identifier
        
        Parameters
        ----------
        initial_catalogue_fullname: string, required
            Initial Catalogue Fullname

        initial_catalogue_alias: string, required
            Initial Catalogue Alias

        initial_catalogue_ident: string, required
            Initial Catalogue Ident
        
        Returns    
        -------
        query_schema: string
            The URL of the created Query Schema
        
        """
        importname = ""
        t = datetime.now()
        workspace = self.create_adql_space()
        self.adqlspace = workspace
        query_schema = self.create_query_schema(workspace)
        
        name = initial_catalogue_fullname
        alias = initial_catalogue_alias
        ident = initial_catalogue_ident
        data = None
        try:   
            if alias!="":
                importname = alias
            else :
                importname = name

            if importname!="":
                data = urllib.parse.urlencode({config.workspace_import_schema_base : ident, config.workspace_import_schema_name : importname}).encode("utf-8")
                req = urllib.request.Request( workspace + config.workspace_import_uri, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"}) 
                response = urllib.request.urlopen(req, data)
        except Exception as e:
            logging.exception(e)
       
        return query_schema
    

    def import_query_schema(self, name, import_schema, workspace):
        """Import a schema into a workspace for querying
        
        Parameters
        ----------
        name: string, required
            Name of schema

        import_schema: string, required
            Schema to import

        workspace: string, required
            Workspace URL
        
        
        """

        try:
            importname = name

            if importname!="":
                data = urllib.parse.urlencode({config.workspace_import_schema_base : import_schema, config.workspace_import_schema_name : importname}).encode("utf-8")
                req = urllib.request.Request( workspace + config.workspace_import_uri, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"}) 
                with urllib.request.urlopen(req, data) as response:
                    response.read()
        except Exception as e:
            logging.exception(e)
            
        return


    def create_ivoa_space(self, ivoa_space_name, url):
        """Create an IVOA resource
        
        Parameters
        ----------
        ivoa_space_name: string, required
            Name of IVOA resource

        url: string, required
            URL of IVOA resource to import
            
        Returns
        -------
        ivoaspace: String
            The IVOA resource URL 
        
        """
        try:
            data = urllib.parse.urlencode({"ivoa.resource.name" : ivoa_space_name , "ivoa.resource.endpoint" : url}).encode("utf-8")
            req = urllib.request.Request( self.endpoint + config.ivoa_resource_create, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" : "public (unknown)"}) 
            with urllib.request.urlopen(req, data) as response:
                ivoaspace =  json.loads(response.read().decode('ascii'))["ident"]
            response.close()
        except Exception as e:
            logging.exception(e)

        return ivoaspace


    def import_vosi(self, vosi_name, ivoa_resource):
        """Import VOSI
        
        Parameters
        ----------
        vosi_name: string, required
            VOSI name

        ivoa_resource: string, required
            IVOA resource
            
        Returns
        -------
        schema: String
            The Schema URL
        
        """
      
        import pycurl
        import cStringIO
        
        buf = cStringIO.StringIO()
        schema = "" 
        try:
           
            c = pycurl.Curl()   
            
            url = ivoa_resource + "/vosi/import"        
            values = [  
                      ("vosi.tableset", (c.FORM_FILE, vosi_name ))]
                       
            c.setopt(c.URL, str(url))
            c.setopt(c.HTTPPOST, values)
            c.setopt(c.WRITEFUNCTION, buf.write)
            c.setopt(pycurl.HTTPHEADER, [ "firethorn.auth.identity",config.test_email,
                                          "firethorn.auth.community","public (unknown)"
                                          ])
            c.perform()
            c.close()
            schema = json.loads(buf.getvalue())[0]["ident"]
            buf.close() 
            
        except Exception as e:
            logging.exception(e)
     
        return schema


    def get_ivoa_schema(self, findname="", ivoa_resource=""):
        """Get IVOA Schema
        
        Parameters
        ----------
        findname: string, required
            name of Schema to find

        ivoa_resource: string, required
            IVOA resource
            
        Returns
        -------
        schemaident: String
            The Schema URL
        
        """


        schemaident=""
        try:
            data = urllib.parse.urlencode({ "ivoa.schema.name" : findname }).encode("utf-8")
            req = urllib.request.Request( ivoa_resource + "/schemas/select", headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email , "firethorn.auth.community" :"public (unknown)"})
            with urllib.request.urlopen(req, data) as response:
                schemaident =  json.loads(response.read().decode('ascii'))["self"]
            response.close()

        except Exception as e:
            logging.exception(e)

        return schemaident   


    def import_ivoa_schema(self, ivoa_resource_name, ivoa_resource_url, ivoa_resource_xml, ivoa_resource_alias, ivoa_schema_import, query_resource):        
        """Import a Schema from an IVOA resource into an ADQL resource
        
        Parameters
        ----------
        ivoa_resource_name: string, required 
            IVOA Resource name

        ivoa_resource_url: string, required
            IVOA Resource URL  

        ivoa_resource_xml: string, required  
            IVOA Resource XML file  
        
        ivoa_resource_alias: string, required  
            IVOA Resource Alias  

        ivoa_schema_import: string, required  
            IVOA Resource Import  

        query_resource: string, required 
            Query Resource 
         
        
        """
        ivoaspace = self.create_ivoa_space(ivoa_resource_name, ivoa_resource_url)
        ivoaschema = self.import_vosi(ivoa_resource_xml, ivoaspace)
        schema = self.get_ivoa_schema(ivoa_schema_import, ivoaspace)
        self.import_query_schema(ivoa_resource_alias, schema, query_resource)
        return

    
    def print_class_vars(self):
        """Print out the class (Firethorn environment) variables
        """
        logging.info("jdbcspace: " + self.jdbcspace)
        logging.info("adqlspace: " + str(self.adqlspace))
        logging.info("adqlschema: " + str(self.adqlschema))
        logging.info("query_schema: " + str(self.query_schema))
        logging.info("schema_name: " + str(self.schema_name))
        logging.info("schema_alias: " + str(self.schema_alias))     
    
    
    def select_by_name(self, name, resource):
        """Select by name
        
        Parameters
        ----------
        name: string, required
            The name of the entity being searched for
            
        resource: string, required 
            Resource to search 
         
        Returns
        -------
        string: string
            The URL of the entity found
        """
        
        try :
            req_exc = urllib.request.Request( resource + "/select?name=" + name, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" :"public (unknown)"})

            #with urllib.request.urlopen(req_exc) as response:
            #    response_exc_json =  response.read().decode('utf-8')
        except Exception as e:
            logging.exception(e)      
              
        return "http://localhost:8081/firethorn/adql/schema/2308582"
    
    
    def get_tables(self, schemaname):
        """Get tables
        
        Parameters
        ----------
        schemaname: string, required
            The name of the schema for which to return the children tables
         
        Returns
        -------
        table_list: list
            List of table names
        """
        schemaident = self.select_by_name(schemaname, self.adqlspace)
        response_json = None
        table_list = []
        
        try :
            req_exc = urllib.request.Request( schemaident + "/tables/select", headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" :"public (unknown)"})
            with urllib.request.urlopen(req_exc) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
            for val in response_json:
                table_list.append(val["name"])
     
        except Exception as e:
            logging.exception(e)
            
        return table_list

    
    def get_columns(self, tablename):
        """Get columns
        
        Parameters
        ----------
        tablename: string, required
            The name of the table for which to return the children columns
         
        Returns
        -------
        column_list: list
            List of column names
        """
        
        tableident = self.select_by_name(tablename, self.adqlspace)
        response_json = None
        column_list = []
        
        try :
            req_exc = urllib.request.Request( tableident + "/column/select", headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" :"public (unknown)"})
            with urllib.request.urlopen(req_exc) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
            for val in response_json:
                column_list.append(val["name"])
     
        except Exception as e:
            logging.exception(e)
            
        return column_list
    
    
    def get_attribute(self, ident, attr):
        """Get an attribute of a JSON HTTP resource
        
        Parameters
        ----------
        ident: string, required
            The URL being queried

        attr: string, required
            The attribute of the JSON response being asked for  
                   
        Returns
        -------
        attr_val: string
            Value of the attribute requested
        """
        
        attr_val = []
        try :
            req_exc = urllib.request.Request( ident, headers={"Accept" : "application/json", "firethorn.auth.identity" : config.test_email, "firethorn.auth.community" :"public (unknown)"}).encode("utf-8")
            with urllib.request.urlopen(req_exc) as response:
                response_exc_json =  response.read().decode('ascii')       
            attr_val = json.loads(response_exc_json)[attr]
        except Exception as e:
            logging.exception(e)
        return attr_val

