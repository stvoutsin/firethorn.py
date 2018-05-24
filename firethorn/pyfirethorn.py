try:
    import logging
    from models.resource import Resource 
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


    __predefined_resources__ = {"OSA": config.osa_endpoint}
    __id__ = ""
    
    
    def __init__(self, username=None, password=None, endpoint = config.endpoint, community=None):
        print("Firethorn: init()")
        print("  username  [{}]".format(username))
        print("  password  [{}]".format(password))
        print("  community [{}]".format(community))
        print("  endpoint  [{}]".format(endpoint))
        self.endpoint = endpoint
        print("Firethorn: creating engine")
        self.firethorn_engine =  FirethornEngine(endpoint=endpoint)
        print("Firethorn: engine created")

        print("Firethorn: checking username")
        if username!=None:
            # Why use a different login() method ?
            print("Firethorn: engine.loggin()")
            self.firethorn_engine.login(username, password, community)            
            print("Firethorn: engine.loggin() done")
        else:
            print("Firethorn: creating temp account")
            self.firethorn_engine.create_temporary_account()
            print("Firethorn: temp account created")
        print("Firethorn: init() done")
        print("  endpoint  [{}]".format(self.endpoint))
        return        


    def login(self, username=None, password=None, community=None):
        """
        Login 
        """
        print("Firethorn: login()")
        print("  username  [{}]".format(username))
        print("  password  [{}]".format(password))
        print("  community [{}]".format(community))
        if username!=None:
            print("Firethorn: calling engine.login()")
            if (self.firethorn_engine.login(username, password, community)):
                print("Firethorn: engine.login() passed")
                print("  username  [{}]".format(self.firethorn_engine.account.username))
                print("  password  [{}]".format(self.firethorn_engine.account.password))
                print("  community [{}]".format(self.firethorn_engine.account.community))
                return "Successfully logged in as: " + username
            else:
                print("Firethorn: engine.login() failed")
                print("  username  [{}]".format(self.firethorn_engine.account.username))
                print("  password  [{}]".format(self.firethorn_engine.account.password))
                print("  community [{}]".format(self.firethorn_engine.account.community))
                # We don't know why, just true/false
                return "Incorrect username/password"
        else:
            return "Please enter a valid username"
            
        return


    def identity(self):
        """
        Get the name of the identity currently logged in
        """
        return self.firethorn_engine.account
    
    
    def get_public_resources(self):
        """ Get public resource list 
        """
        resource_list = []
        
        for resource in self.__predefined_resources__:
            resource_list.append(Resource(adql_resource=AdqlResource(url=self.__predefined_resources__.get(resource,None), account=self.firethorn_engine.account)))             
      
        return resource_list
    
    
    def get_public_resource_names(self):
        """ Get a list of available public predefined resources
               
        Returns
        -------
        list : list
            List of resource names (strings)
        """
        return list(self.__predefined_resources__.keys())
    
    
    def get_public_resource_by_name(self, name):
        """ Select a resource from the public list of resources, by name
        
        Parameters
        ----------
        name : string, optional
            The workspace name
                      
        Returns
        -------
        Workspace : Workspace
            A copy of the selected Resource object
        """
        return Resource(adql_resource=AdqlResource(url=self.__predefined_resources__.get(name,None), account=self.firethorn_engine.account))
    
    
    def new_workspace(self, name=None):
        """ Create a new workspace
        
        Parameters
        ----------
        name : string, optional
            The workspace name
                      
        Returns
        -------
        Resource : Resource
            The newly created Resource
        """
        
        resource = self.firethorn_engine.create_adql_resource(name)
        return Resource(adql_resource=resource)
    
    

    


