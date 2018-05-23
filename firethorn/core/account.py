'''
Created on Feb 8, 2018

@author: stelios
'''

import urllib
import config as config
import logging

class Account(object):
    '''
    classdocs
    '''

    
    def __init__(self, username=None, password=None, community=None, endpoint=config.endpoint):
        print("Account: init()")
        print("  username  [{}]".format(username))
        print("  password  [{}]".format(password))
        print("  community [{}]".format(community))
        print("  endpoint  [{}]".format(endpoint))
        self.username = username
        self.password = password
        self.community = community     
        self.logged_in = False
        self.endpoint = endpoint
        if (self.username!=None):
            print("Account: calling self.login()")
            self.login()
        print("Account: init() done")
        print("  logged_in [{}]".format(self.logged_in))
        print("  username  [{}]".format(self.username))
        print("  password  [{}]".format(self.password))
        print("  community [{}]".format(self.community))
        print("  endpoint  [{}]".format(self.endpoint))
        
        
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

    @property
    def logged_in(self):
        return self.__logged_in
        
        
    @logged_in.setter
    def logged_in(self, logged_in):
        self.__logged_in = logged_in
   

    def login(self):
        self.logged_in = False
        print("Account: login()")
        print("  logged_in [{}]".format(self.logged_in))
        print("  username  [{}]".format(self.username))
        print("  password  [{}]".format(self.password))
        print("  community [{}]".format(self.community))
        print("  endpoint  [{}]".format(self.endpoint))
        req = urllib.request.Request(self.endpoint + config.system_info, headers=self.get_identity_as_headers())
        with urllib.request.urlopen(req) as response:
            response.read().decode('utf-8')     

            if (response.getcode()!=200):
                self.logged_in = False
                print("Login FAIL")
                print("  response code [{}]".format(response.getcode()))
            else:
                print("Request PASS")
                print("  response code [{}]".format(response.getcode()))
                response_username  = response.info()["firethorn.auth.username"]
                response_community = response.info()["firethorn.auth.community"]
                print("  response username  [{}]".format(response_username))
                print("  response community [{}]".format(response_community))
                if ((self.username != None) and (self.username != response_username)):
                    self.logged_in = False
                    print("Login FAIL")
                    print("Usernames don't match")
                elif ((self.community != None) and (self.community != response_community)):
                    self.logged_in = False
                    print("Login FAIL")
                    print("Communities don't match")
                else:
                    self.logged_in = True
                    self.username  = response_username
                    self.community = response_community
                    print("Login PASS")
                    print("  logged_in [{}]".format(self.logged_in))
                    print("  username  [{}]".format(self.username))
                    print("  password  [{}]".format(self.password))
                    print("  community [{}]".format(self.community))
        
        print("Account: login() done")
        print("  logged_in [{}]".format(self.logged_in))
        print("  username  [{}]".format(self.username))
        print("  password  [{}]".format(self.password))
        print("  community [{}]".format(self.community))
        print("  endpoint  [{}]".format(self.endpoint))
        return self.logged_in
            
        
    def get_identity_as_headers(self):
        """
        Get a Dictionary of values representing a Identity, to be used for Firethorn Requests
        """
        if (self.username!=None):
            if (self.password!=None and self.username!=None and self.community!=None):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.community==None):
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username, "firethorn.auth.password" : self.password}
            elif (self.password==None):
                return {"Accept" : "application/json", "firethorn.auth.community" : self.community, "firethorn.auth.username" : self.username}
            else:
                return {"Accept" : "application/json", "firethorn.auth.username" : self.username}
        else :
            return {"Accept" : "application/json"}
        
        
    def __str__(self):
        """ Print User as string
        """
        return 'Username: %s ' %(self.username) 
        
