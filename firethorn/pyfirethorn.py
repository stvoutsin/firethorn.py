import logging
from models.query import Query
from models.workspace import Workspace 
from core.firethorn_engine import FirethornEngine
import time

class Firethorn(object):
    """
    Firethorn client class
    Query WFAU and VO services
     

    Attributes:
    """


    __predefined_workspaces__ = {"OSA": Workspace(ident="http://localhost:8081/firethorn/adql/resource/2308497",queryspace="http://localhost:8081/firethorn/adql/schema/5210118")}
    __id__ = ""
    
    
    def __init__(self, user=None, password=None, endpoint = None, driver="net.sourceforge.jtds.jdbc.Driver"):
        '''
        Constructor
        
        Parameters
        ----------
        '''
        self.user = user
        self.password = password
        self.endpoint = endpoint
        self.firethorn_engine =  FirethornEngine(driver = driver, endpoint=endpoint)

        return        


    def get_workspace(self, name):
        """
        Parameters
        ----------
                    
        Returns
        -------
        """
        return self.__predefined_workspaces__.get(name,None)
    
    
    def new_workspace(self, name=None):
        """
        Parameters
        ----------
                    
        Returns
        -------
        
        """
        
        resource = self.firethorn_engine.create_adql_space(name)
        queryspace = self.firethorn_engine.create_query_schema(resource)

        return Workspace(resource, queryspace)
    
    
    def get_public_workspaces(self):
        """
        Parameters
        ----------
                    
        Returns
        -------
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


