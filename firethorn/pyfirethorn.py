import account
try:
    import logging
    from models.workspace import Workspace 
    from core.firethorn_engine import FirethornEngine
    import config as config
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
        
    """


    __predefined_workspaces__ = {"OSA": config.osa_endpoint}
    __id__ = ""
    
    
    def __init__(self, username=None, password=None, endpoint = config.default_endpoint, community=None):
        self.endpoint = endpoint
        self.firethorn_engine =  FirethornEngine(endpoint=endpoint)

        if username!=None:
            self.firethorn_engine.login(username, password, community)            
        else:
            self.firethorn_engine.create_temporary_account()
        return        


    def login(self, username=None, password=None, community=None):
        """
        Login 
        """
        if username!=None:
            if (self.firethorn_engine.login(username, password, community)):
                return "Successfully logged in as: " + username
            else:
                return "Incorrect username/password"
        else:
            return "Please enter a valid username"
            
        return


    def identity(self):
        """
        Get the name of the identity currently logged in
        """
        return self.firethorn_engine.account
    
    
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
        return Workspace(adql_resource=AdqlResource(url=self.__predefined_workspaces__.get(name,None),account=self.firethorn_engine.account))
    
    
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
        return Workspace(adql_resource=resource)
    
    
    def get_public_workspaces(self):
        """ Get a list of available public predefined workspaces
               
        Returns
        -------
        list : list
            List of workspace names (strings)
        """
        return list(self.__predefined_workspaces__.keys())
    


