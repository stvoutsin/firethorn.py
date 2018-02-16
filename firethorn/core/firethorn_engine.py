'''
Created on Jul 22, 2013

@author: stelios
'''

try:
    import urllib
    import json
    import logging
    from datetime import datetime
    import config as config
    from models.identity import Identity
    import pycurl
    import io
    import uuid
    import urllib.request
    from jdbc.jdbc_resource import JdbcResource
    from models.jdbc.jdbc_schema import JdbcSchema
    from models.adql.adql_resource import AdqlResource
    from models.ivoa.ivoa_resource import IvoaResource
except Exception as e:
    logging.exception(e)


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
        Ali1as of the ADQL Schema URL to initialize with
        
    driver: string, optional
        Driver used in Firethorn JDBC Connections    

    endpoint: string, optional
        Endpoint used for the Firethorn services    

    """


    def __init__(self, jdbcspace="", adqlspace="", adqlschema="", query_schema="", schema_name="", schema_alias="", driver="", endpoint = "" , identity = None, **kwargs):

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
        self.queryspace = None
        self.identity = identity
    
    
    def login(self, username=None, password=None, community=None):
        """
        Login a User
        Creates a User object attached to the Firethorn Engine
        
        Parameters
        ----------
        username: string, optional
            Username
            
        password: string, optional
            Password
            
        community: string, optional
            Community
        """    
          
        loggedin = False
        
        try :
            
            new_user = Identity(username, password, community)
            req = urllib.request.Request(self.endpoint + config.system_info, headers=new_user.get_identity_as_headers())

            with urllib.request.urlopen(req) as response:
                response.read().decode('utf-8')     
                if (response.getcode()==200):
                    loggedin = True
                else:
                    self.identity = Identity(None,None,None)
        except Exception as e:
            self.identity = Identity(None,None,None)
            pass
            
        if loggedin:        
            self.identity = Identity(username, password, community )
            return True
        else : 
            return False


    def system_info_check(self):
        """
        Check system info
        """
        try :
            req = urllib.request.Request(self.endpoint + config.system_info, headers=self.identity.get_identity_as_headers())
            with urllib.request.urlopen(req) as response:
                return (json.loads(response.read().decode('utf-8')))
        except Exception as e:
            logging.exception(e)
            
        return {} 
                     

    def create_temporary_user(self):
        """
        Create a temporary user
        
        """            
        username = None
        community = None
        try :
            
            req = urllib.request.Request(  self.endpoint + config.system_info, headers={"Accept" : "application/json"})
            with urllib.request.urlopen(req) as response:
                info = response.info()
                for header in info:
                    if (header.lower()=="firethorn.auth.username"):
                        username =  info[header]
                    if (header.lower()=="firethorn.auth.community"):
                        community =  info[header]
                        
        except Exception as e:
            logging.exception(e)
            
        self.user = Identity(username = username, community = community)
        
         
    def create_jdbc_resource(self, resourcename ,resourceurl, catalogname, jdbc_name, jdbc_resource_user="", jdbc_resource_pass=""):
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
                                     "jdbc.connection.url" : resourceurl,	
                                     "jdbc.resource.catalog" : catalogname,
                                     "jdbc.resource.name" : jdbc_name,
                                     "jdbc.connection.driver" : self.driver,
                                     "jdbc.connection.user" : jdbc_resource_user,
                                     "jdbc.connection.pass" : jdbc_resource_pass
                                    }).encode("utf-8")
    

            else :
                data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/jdbc-resource-1.0.json'] : resourcename ,
                                     "jdbc.connection.url" : resourceurl,
                                     "jdbc.catalog.name" : catalogname,
                				     "jdbc.connection.driver" : self.driver,

                                    }).encode("utf-8")



            req = urllib.request.Request( self.endpoint + config.jdbc_creator, headers=self.identity.get_identity_as_headers())
            response = urllib.request.urlopen(req,data)
            jdbcspace = json.loads(response.read().decode("utf-8"))
            response.close()
            
        except Exception as e:
            logging.exception(e)
            
        return JdbcResource(json_object=jdbcspace, firethorn_engine=self)    
    
    
    
    def select_jdbc_resource_by_name(self, name):
        """JDBC Resource select by catalog and schema name
        
        Parameters
        ----------
        name: string, required
            The JDBC Resource name

        Returns
        -------
        JdbcResource: JdbcResource
            The JdbcResource found
        """
        
        response_json = {}
        return     
            

    def select_jdbc_schema_by_name(self, jdbcurl, catalog, schema):
        """JDBC Schema select by catalog and schema name
        
        Parameters
        ----------
        catalog: string, required
            The JDBC Catalog name
            
        schema: string, required 
            The JDBC Schema name 
         
        Returns
        -------
        JdbcSchema: JdbcSchema
            The JdbcSchema found
        """
        
        response_json = {}
        
        try :
            data = urllib.parse.urlencode({config.jdbc_schema_catalog : catalog, config.jdbc_schema_schema : schema }).encode("utf-8")
            req = urllib.request.Request( jdbcurl + "/schemas/select", headers=self.identity.get_identity_as_headers())

            with urllib.request.urlopen(req, data) as response:
                response_json =  json.loads(response.read().decode('utf-8'))
                
        except Exception as e:
            logging.exception(e)      
            
        return JdbcSchema(json_object = response_json, firethorn_engine=self)
    
    
    def select_jdbc_resource_by_ident(self, ident):
        return JdbcResource(url=ident, firethorn_engine=self)

    
    def create_adql_resource(self, adqlspacename=None):
        """Create an ADQL Resource
        
        Parameters
        ----------
        adqlspacename: string, required
            ADQL Space name 
            
        
        Returns    
        -------
        adqlresource: AdqlResource
            The new AdqlResource 
        
        """
        
        adqlresource = ""
        
        try:
            ### Create workspace
            if adqlspacename==None:
                t = datetime.now()
                adqlspacename = 'workspace-' + t.strftime("%y%m%d_%H%M%S") 
            data = urllib.parse.urlencode({config.resource_create_name_params['http://data.metagrid.co.uk/wfau/firethorn/types/entity/adql-resource-1.0.json'] : adqlspacename}).encode("utf-8")
            req = urllib.request.Request( self.endpoint + config.workspace_creator, headers=self.identity.get_identity_as_headers())
            with urllib.request.urlopen(req, data) as response:
                adqlresource =  json.loads(response.read().decode('utf-8'))
            response.close()
        except Exception as e:
            logging.exception(e)
            
        return AdqlResource(json_object = adqlresource, firethorn_engine=self)
                         
                         
    def select_adql_resources(self):
        """
        Select all ADQL Resources
        """
        adqlresource = {}
        
        try:
 
            req = urllib.request.Request( self.endpoint + config.get_adql_resources_url, headers=self.identity.get_identity_as_headers())
            with urllib.request.urlopen(req) as response:
                adqlresources =  json.loads(response.read().decode('utf-8'))
            response.close()
        except Exception as e:
            logging.exception(e)
            
        return adqlresources
    
    
    def select_adql_resource_by_name(self, resource_name):
        """
        Select an ADQL resource by name
        """
        return
    
    
    def select_adql_resource_by_ident(self, ident):
        """
        Select an ADQL resource by ident
        """
        return AdqlResource(firethorn_engine=self, url=ident)


    def select_ivoa_resources(self):
        """
        Select all ADQL Resources
        """
        ivoaresources = {}
        
        try:
 
            req = urllib.request.Request( self.endpoint + config.get_ivoa_resources_url, headers=self.identity.get_identity_as_headers())
            with urllib.request.urlopen(req) as response:
                ivoaresources =  json.loads(response.read().decode('utf-8'))
            response.close()
        except Exception as e:
            logging.exception(e)
            
        return ivoaresources
    
    
    def select_ivoa_resource_by_name(self, name):
        return 
    
    
    def select_ivoa_resource_by_ident(self, ident):
        """
        Select an IVOA resource by ident
        """
        return IvoaResource(firethorn_engine=self, url=ident)
    

    def create_ivoa_resource(self, ivoa_space_name, url):
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
        
        ivoa_resource = None
        try:
            data = urllib.parse.urlencode({"ivoa.resource.name" : ivoa_space_name , "ivoa.resource.endpoint" : url}).encode("utf-8")
            req = urllib.request.Request( self.endpoint + config.ivoa_resource_create, headers=self.identity.get_identity_as_headers()) 
            with urllib.request.urlopen(req, data) as response:
                ivoa_resource =  json.loads(response.read().decode('utf-8'))
            response.close()
        except Exception as e:
            logging.exception(e)

        return IvoaResource(firethorn_engine=self, json_object=ivoa_resource)
    
    
    def make_identity (self, identity):
        return Identity (identity)
    
    
    def _get_attribute(self, ident, attr):
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
            req_exc = urllib.request.Request( ident, headers=self.identity.get_identity_as_headers()).encode("utf-8")
            with urllib.request.urlopen(req_exc) as response:
                response_exc_json =  response.read().decode('utf-8')       
            attr_val = json.loads(response_exc_json)[attr]
        except Exception as e:
            logging.exception(e)
        return attr_val
    


    def get_json(self, ident):
        """Select a JSON HTTP resource
        
        Parameters
        ----------
        ident: string, required
            The URL being queried

        """
        
        json_result = {}
        
        try :
            req_exc = urllib.request.Request( ident, headers=self.identity.get_identity_as_headers())
            with urllib.request.urlopen(req_exc) as response:
                json_result =  json.loads(response.read().decode('utf-8'))
     
        except Exception as e:
            logging.exception(e)
            
        return json_result
    


