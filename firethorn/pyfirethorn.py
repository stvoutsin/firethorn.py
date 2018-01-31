import logging
from models.query import Query
from models.workspace import Workspace 
from models import User
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


    __predefined_workspaces__ = {"OSA": "http://localhost:8081/firethorn/adql/resource/54"}
    __id__ = ""
    
    
    def __init__(self, username=None, password=None, endpoint = "http://localhost:8081/firethorn", community=None, driver="net.sourceforge.jtds.jdbc.Driver"):
        self.endpoint = endpoint
        self.firethorn_engine =  FirethornEngine(driver = driver, endpoint=endpoint)

        if username!=None:
            self.firethorn_engine.login(username, password, community)            
        else:
            self.firethorn_engine.create_temporary_user()
        return        


    def login(self, username=None, password=None, community=None):
        if username!=None:
            if (self.firethorn_engine.login(username, password, community)):
                print ("Successfully logged in as: " + username)
            else:
                print ("Incorrect username/password")
        else:
            print ("Please enter a valid username")
            
        return


    def username(self):
        return self.firethorn_engine.username
    
    
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
        
        resource = self.firethorn_engine.create_adql_space(name)
        return Workspace(resource, firethorn_engine = self.firethorn_engine)
    
    
    def get_public_workspaces(self):
        """ Get a list of available public predefined workspaces
               
        Returns
        -------
        list : list
            List of workspace names (strings)
        """
        return list(self.__predefined_workspaces__.keys())
    


if __name__ == "__main__":
    ft = Firethorn()
    ft.login("", "", "")
    #print (ft.get_public_workspaces())
    osa = ft.get_workspace("OSA")
    """
    myquery = osa.query_async("SELECT * FROM ATLASDR1.Filter")
    myquery.run()
    while myquery.status()=="RUNNING" or myquery.status()=="READY":
        print (myquery.status())
        time.sleep(5)
    
    print (myquery.results().as_astropy())
    """
    wspace = ft.new_workspace("ATLAS")
    wspace.import_schema(osa.get_schema("ATLASDR1"))
    #print (wspace.get_tables("ATLASDR1"))

    
    
    qry = wspace.query("Select top 3 * from ATLASDR1.Filter")
    print (qry.results().as_astropy())
    print (qry.results())
    #print (osa.get_columns(table="TAP_SCHEMA.tables"))
    #table = osa.query("SELECT TOP 10 table_name as tname from TAP_SCHEMA.tables")
    #print (table.get_table())
    

