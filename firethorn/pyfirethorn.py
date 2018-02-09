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
    print ("atlas_jdbc: " +  str(atlas_jdbc))
    print ("Ident: " + atlas_jdbc.ident())
    print ("Name: " + atlas_jdbc.name())
    print ("Owner: " + atlas_jdbc.owner())
    print ("URL: " + atlas_jdbc.url)   
    
    
    print ("select_schemas() : ")
    print (atlas_jdbc.select_schemas()) 
    print ("select_schema_by_ident(): ")
    print (atlas_jdbc.select_schema_by_ident("http://localhost:8081/firethorn/jdbc/schema/21061")) 
    print ("select_schema_by_name(): ")
    print (atlas_jdbc.select_schema_by_name("ATLASDR1", "dbo")) 
    print ("create_schema(): ") 
    print (atlas_jdbc.create_schema("dbo", "mySchema") ) ## ???? Not implemented yet
    
 
    
    # Locate the JdbcSchema based on catalog and schema name. 
    catalog="ATLASDR1"
    schema="dbo"
    
    print ("select_jdbc_resource_by_name(): ")
    print (ft.firethorn_engine.select_jdbc_resource_by_name(catalog)) ## ???  ## ???? Not implemented yet
    print ("select_jdbc_resource_by_ident(): ")
    print ( ft.firethorn_engine.select_jdbc_resource_by_ident(atlas_jdbc.url))
    
    
    
    atlas_jdbc_schema = ft.firethorn_engine.select_jdbc_schema_by_name(atlas_jdbc.url, catalog, schema)
    print ("atlas_jdbc_schema: ")
    print (atlas_jdbc_schema)
    print ("resource(): ")
    print (atlas_jdbc_schema.resource())
    print ("catalog_name(): " + atlas_jdbc_schema.catalog_name())
    print ("select_tables(): ")
    print (atlas_jdbc_schema.select_tables())
    print ("select_table_by_ident(): ")
    print (atlas_jdbc_schema.select_table_by_ident("http://localhost:8081/firethorn/jdbc/table/24105"))
    print ("select_table_by_name(): ") 
    print (atlas_jdbc_schema.select_table_by_name("Filter")) ## ???? ## ???? Not implemented yet
    print ("create_table(): ") 
    print (atlas_jdbc_schema.create_table("myTable")) ## ???? ## ???? Not implemented yet



    filter_jdbc_table = atlas_jdbc_schema.select_table_by_ident("http://localhost:8081/firethorn/jdbc/table/24105")
    print ("name(): " + filter_jdbc_table.name())
    print ("ident(): " + filter_jdbc_table.ident())
    print ("resource(): ") 
    print (filter_jdbc_table.resource())
    print ("schema(): ") 
    print (filter_jdbc_table.schema())
    print ("select_columns(): ") 
    print (filter_jdbc_table.select_columns())
    print ("select_column_by_ident(): ")
    print (filter_jdbc_table.select_column_by_ident("http://localhost:8081/firethorn/jdbc/column/19980"))
    print ("select_column_by_name(): ")
    print (filter_jdbc_table.select_column_by_name("filterID")) ## ?? Not implemented yet
    print ("create_column(): ")
    print (filter_jdbc_table.create_column("myColumn")) ## ?? Not implemented yet

    
    
    filterID_jdbc_column = filter_jdbc_table.select_column_by_ident("http://localhost:8081/firethorn/jdbc/column/19980")
    print ("url: " + filterID_jdbc_column.url)
    print ("name(): " + filterID_jdbc_column.name())
    print ("ident(): " + filterID_jdbc_column.ident())
    print ("owner(): " + filterID_jdbc_column.owner())
    print ("resource(): ")
    print (filterID_jdbc_column.resource())
    print ("schema(): ")
    print (filterID_jdbc_column.schema())
    print ("table(): ")
    print (filterID_jdbc_column.table())
    print ("type(): " + filterID_jdbc_column.type())
    print ("arraysize(): " + str(filterID_jdbc_column.size()))
    print ("ucd(): " + str(filterID_jdbc_column.ucd()))
    print ("utype(): " + str(filterID_jdbc_column.utype()))


    
    # Create an empty AdqlResource to represent the local JDBC database.
    adqlname="ATLAS ADQL resource"
    atlas_adql = ft.firethorn_engine.create_adql_resource(adqlname)
    print ("atlas_adql: ")
    print (atlas_adql) 
    print (ft.firethorn_engine.select_adql_resource_by_ident("http://localhost:8081/firethorn/adql/resource/26683"))
    print (ft.firethorn_engine.select_adql_resource_by_name("ATLAS")) ## ?? Not implemented yet


    
    # Import the mapping between JDBC and ADQL tables.
    metadoc="https://raw.githubusercontent.com/stvoutsin/firethorn.py/master/firethorn/meta/ATLASDR1_TablesSchema.xml"
    atlas_adql_schema = atlas_adql.import_jdbc_schema(atlas_jdbc_schema, metadoc=metadoc)
    atlas_adql_schemav2 = atlas_adql.import_jdbc_schema(atlas_jdbc_schema, schema_name="atlasV2")
    print (atlas_adql_schema)
    print (atlas_adql_schemav2)

    """
    
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
