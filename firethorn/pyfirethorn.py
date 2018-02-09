try:
    import logging
    from models.query import Query
    from models.workspace import Workspace 
    from models import Identity
    from core.firethorn_engine import FirethornEngine
    import config as config
    import time
    from adql.adql_resource import AdqlResource
    
except Exception as e:
    logging.exception(e)

class Firethorn(object):
    """
    Firethorn client class
    Query WFAU and VO services
    
    Parameters
    ----------
    username : string, optional
        Username
        
    password : string, optional
        Password
    
    community : string, optional
        Community
        
    endpoint : string, optional
        URL Endpoint to intialise Firethorn class with
        
    driver : string, optional
        Driver used in Firethorn JDBC Connections    
    """


    __predefined_workspaces__ = {"OSA": config.default_endpoint + "/firethorn/adql/resource/56"}
    __id__ = ""
    
    
    def __init__(self, username=None, password=None, endpoint = config.default_endpoint + "/firethorn", community=None, driver="net.sourceforge.jtds.jdbc.Driver"):
        self.endpoint = endpoint
        self.firethorn_engine =  FirethornEngine(driver = driver, endpoint=endpoint)

        if username!=None:
            self.firethorn_engine.login(username, password, community)            
        else:
            self.firethorn_engine.create_temporary_user()
        return        


    def login(self, username=None, password=None, community=None):
        """
        Login 
        """
        if username!=None:
            if (self.firethorn_engine.login(username, password, community)):
                return "Successfully logged in as: " + username
            else:
                print("Incorrect username/password")
                return "Incorrect username/password"
        else:
            return "Please enter a valid username"
            
        return


    def identity(self):
        """
        Get the name of the identity currently logged in
        """
        if (self.firethorn_engine.identity!=None and self.firethorn_engine.identity.username!=None):
            return (self.firethorn_engine.identity.username)
        else :
            return "anonymous identity"
    
    
    def get_workspace(self, name):
        """ Select a workspace from the predefined list of workspaces, by name
        
        Parameters
        ----------
        name : string, optional
            The workspace name
                      
        Returns
        -------
        Workspace : Workspace
            A copy of the selected Workspace object
        """
        return Workspace(ident=self.__predefined_workspaces__.get(name,None), firethorn_engine=self.firethorn_engine)
    
    
    def new_workspace(self, name=None):
        """ Create a new workspace
        
        Parameters
        ----------
        name : string, optional
            The workspace name
                      
        Returns
        -------
        Workspace : Workspace
            The newly created Workspace
        """
        
        resource = self.firethorn_engine.create_adql_resource(name)
        return AdqlResource(resource, firethorn_engine = self.firethorn_engine)
    
    
    def get_public_workspaces(self):
        """ Get a list of available public predefined workspaces
               
        Returns
        -------
        list : list
            List of workspace names (strings)
        """
        return list(self.__predefined_workspaces__.keys())
    


if __name__ == "__main__":
    ft = Firethorn(endpoint=config.default_endpoint + "/firethorn")
    ft.login("orinoco", "wombleden", "wombles")
    #print (ft.get_public_workspaces())
    #osa = ft.get_workspace("OSA")
    #print (ft.identity())
    
    
    # System info check
    #print (ft.firethorn_engine.system_info_check())
    
    
    #  Create a JdbcResource to represent the local JDBC database.
    jdbc_name="ATLAS JDBC resource"
    jdbc_url="jdbc:jtds:sqlserver://" + config.datahost + "/ATLASDR1"
    atlas_jdbc = ft.firethorn_engine.create_jdbc_resource("ATLAS" , config.dataurl, "ATLASDR1", jdbc_name, config.datauser, config.datapass)
    print (str(atlas_jdbc))
    print (atlas_jdbc.ident())
    print (atlas_jdbc.name())
    print (atlas_jdbc.owner())
    print (atlas_jdbc.url)   
    
    print (atlas_jdbc.select_schemas()) 
    print (atlas_jdbc.select_schema_by_ident("http://localhost:8081/firethorn/jdbc/schema/21061")) 
    print (atlas_jdbc.select_schema_by_name("ATLASDR1", "dbo")) 
     
    
    # Create an empty AdqlResource to represent the local JDBC database.
    adqlname="ATLAS ADQL resource"
    atlas_adql = ft.firethorn_engine.create_adql_resource(adqlname)
    print (atlas_adql)
 
 
    
    # Locate the JdbcSchema based on catalog and schema name. 
    catalog="ATLASDR1"
    schema="dbo"
    
    print (ft.firethorn_engine.select_jdbc_resource_by_name(catalog)) ## ???
    print (ft.firethorn_engine.select_jdbc_resource_by_ident(atlas_jdbc.url))
    
    atlas_jdbc_schema = ft.firethorn_engine.select_jdbc_schema_by_name(atlas_jdbc.url, catalog, schema)
    #print (atlas_jdbc_schema)
 

    
    """
    # Import the mapping between JDBC and ADQL tables.
    metadoc="meta/ATLASDR1_TablesSchema.xml"
    atlas_adql_schema = ft.firethorn_engine.import_jdbc_metadoc(atlas_adql_url, atlas_jdbc_schema, metadoc)
    print (atlas_adql_schema)
    
    
    # Create an IvoaResource to represent the GAIA TAP resource.
    tapname="GAIA TAP service"
    tapurl="http://gea.esac.esa.int/tap-server/tap"
    gaia_ivoa_resource = ft.firethorn_engine.create_ivoa_space(tapname, tapurl)
    print (gaia_ivoa_resource)
    
    
    # Import the VOSI Metadata
    vosifile='meta/vosi/gaia/gaia-tableset.xml'
    gaia_schema = ft.firethorn_engine.import_vosi(vosifile, gaia_ivoa_resource)
    print (gaia_schema)
    
    
    # Create a new ADQL resource to act as a workspace.
    adqlname="Query workspace"
    queryspace = ft.firethorn_engine.create_adql_resource(adqlname)
    print(queryspace)
    
    
    # Find the AtlasDR1 schema by name.
    selector="ATLASDR1"
    atlas_schema = ft.firethorn_engine.select_by_name(selector, atlas_adql_url)
    print (atlas_schema)
    
    
    # Find the Gaia DR1 schema by name.
    selector="gaiadr1"
    gaia_schema = ft.firethorn_engine.select_ivoa_schema(selector, gaia_ivoa_resource)
    print (gaia_schema)
    
    
    # Add the Atlas DR1 schema.
    name="ATLASDR1"
    ft.firethorn_engine.import_schema(name, atlas_schema, queryspace)
    
    
    # Add the Gaia DR1 schema.
    name="GAIADR1"
    base=gaia_schema
    ft.firethorn_engine.import_schema(name, base, queryspace)
    
    
    # List the workspace schema.
    print (ft.firethorn_engine.list_schemas(queryspace))

    

    myquery = osa.query_async("SELECT top 1 filterID FROM ATLASDR1.Filter")
    myquery.run()
    while myquery.status()=="RUNNING" or myquery.status()=="READY":
        print (myquery.status())
        time.sleep(5)
    
    print (myquery.results().as_astropy())
    
    wspace = ft.new_workspace("ATLAS")
    wspace.import_schema(osa.get_schema("ATLASDR1"))
    print (wspace.get_tables("ATLASDR1"))

    
    
    qry = wspace.query("Select top 3 * from ATLASDR1.Filter")
    print (qry.results().as_astropy())
    
    #print (osa.get_columns(table="TAP_SCHEMA.tables"))
    #table = osa.query("SELECT TOP 10 table_name as tname from TAP_SCHEMA.tables")
    #print (table.get_table())
    """
