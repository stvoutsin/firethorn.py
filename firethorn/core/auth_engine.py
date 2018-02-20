'''
Created on Feb 8, 2018

@author: stelios
'''

class AuthEngine(object):
    '''
    classdocs
    '''

    
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
        
        
    def get_identity_as_headers(self):
        """
        Get a Dictionary of values representing a Identity, to be used for Firethorn Requests
        """
        if (self.username):
            if (self.password and (self.username)):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.password):
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.community):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username}
            else:
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username}
        else :
            return {"Accept" : "application/json"}
        
        
    def __str__(self):
        """ Print Identity as string
        """
        return 'Username: %s\nPassword: %s\nCommunity: %s\n ' %(self.username, self.password, self.community) 

        