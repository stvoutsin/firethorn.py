import logging
from models.query import Query
from models.workspace import Workspace 
from core.firethorn_engine import FirethornEngine
import time

class Firethorn(object):
    """
    Firethorn client class
    Query WFAU and VO services
    
    Parameters
    ----------
    user : string, optional
        Username
        
    password : string, optional
        Password
        
    endpoint : string, optional
        URL Endpoint to intialise Firethorn class with
        
    driver : string, optional
        Driver used in Firethorn JDBC Connections    
    """


    __predefined_workspaces__ = {"OSA": Workspace(ident="http://localhost:8081/firethorn/adql/resource/2308497",queryspace="http://localhost:8081/firethorn/adql/schema/5210118")}
    __id__ = ""
    
    
    def __init__(self, user=None, password=None, endpoint = None, driver="net.sourceforge.jtds.jdbc.Driver"):
        self.user = user
        self.password = password
        self.endpoint = endpoint
        self.firethorn_engine =  FirethornEngine(driver = driver, endpoint=endpoint)

        return        


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
        return self.__predefined_workspaces__.get(name,None)
    
    
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
        
        resource = self.firethorn_engine.create_adql_space(name)
        queryspace = self.firethorn_engine.create_query_schema(resource)

        return Workspace(resource, queryspace)
    
    
    def get_public_workspaces(self):
        """ Get a list of available public predefined workspaces
               
        Returns
        -------
        list : list
            List of workspace names (strings)
        """
        return list(self.__predefined_workspaces__.keys())
    


if __name__ == "__main__":
    ft = Firethorn(endpoint="http://localhost:8081/firethorn")
    print (ft.get_public_workspaces())

    osa = ft.get_workspace("OSA")
    myquery = osa.query_async("SELECT * FROM ATLASDR1.Filter")
    myquery.run()
    while myquery.status()=="RUNNING" or myquery.status()=="READY":
        time.sleep(5)
        
    print (myquery.results().as_astropy())
    
    wspace = ft.new_workspace("ATLAS")
    wspace.import_schema(osa.get_schema("ATLASDR1"))
    print (wspace.get_tables("ATLASDR1"))

    
    
    qry = wspace.query("Select top 2 * from ATLASDR1.Filter")
    print (qry.results().as_astropy())

    #print (osa.get_columns(table="TAP_SCHEMA.tables"))
    #table = osa.query("SELECT TOP 10 table_name as tname from TAP_SCHEMA.tables")
    #print (table.get_table())


