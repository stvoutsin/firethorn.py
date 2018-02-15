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


    __predefined_workspaces__ = {"OSA": config.osa_endpoint}
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
        return Workspace(adql_resource=AdqlResource(url=self.__predefined_workspaces__.get(name,None),firethorn_engine=self.firethorn_engine), firethorn_engine=self.firethorn_engine)
    
    
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
        return Workspace(adql_resource=resource, firethorn_engine = self.firethorn_engine)
    
    
    def get_public_workspaces(self):
        """ Get a list of available public predefined workspaces
               
        Returns
        -------
        list : list
            List of workspace names (strings)
        """
        return list(self.__predefined_workspaces__.keys())
    


