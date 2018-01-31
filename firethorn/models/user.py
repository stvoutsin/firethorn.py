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