class User(object):
    """User class 
    """
        
    
    def __init__(self, username=None, password=None, community=None):
        self.username = username
        self.password = password
        self.community = community     

    @property
    def username(self):
        return self.__username
        
        
    @username.setter
    def username(self, username):
        self.__username = username


    @property
    def password(self):
        return self.__password
        
        
    @password.setter
    def password(self, password):
        self.__password = password
        
        
    @property
    def community(self):
        return self.__community
        
        
    @community.setter
    def community(self, community):
        self.__community = community
        
    def get_user_as_headers(self):
        if (self.username):
            if (self.password and (self.username)):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.password):
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.community):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username}
            else:
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username}