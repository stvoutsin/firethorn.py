import logging
from models.query import Query
from core.setup_firethorn import FirethornSetupEngine

class Firethorn(object):
    """
    Firethorn client class
    Query WFAU and VO services
     

    Attributes:
    """

    __predefined_workspaces__ = {"OSA": {"resource":"http://localhost:8081/firethorn/adql/resource/2308497", "queryspace":"http://localhost:8081/firethorn/adql/schema/5210118"}}
    __id__ = ""
    
    
    def __init__(self, resource=None, queryspace=None):
        '''
        Constructor
        
        Parameters
        ----------
        '''
        self.resource = resource
        self.queryspace = queryspace
        return        


    @property
    def resource(self):
        return self.__resource
        
        
    @resource.setter
    def resource(self, resource):
        self.__resource = resource


    @property
    def queryspace(self):
        return self.__queryspace
        
        
    @queryspace.setter
    def queryspace(self, queryspace):
        self.__queryspace = queryspace


    def get_workspace(self, name):
        """
        Parameters
        ----------
                    
        Returns
        -------
        """
        return Firethorn(self.__predefined_workspaces__.get(name,None).get("resource",None),self.__predefined_workspaces__.get(name,None).get("queryspace",None))


    def get_public_workspaces(self):
        """
        Parameters
        ----------
                    
        Returns
        -------
        """
        return self.__predefined_workspaces__.keys()
    
    
    def new_query(self, query=""):
        """
        Run a query on the imported resources
        
        Parameters
        ----------
        query : str, required
            The query string
            
        Returns
        -------
        query : `Query`
        """
        
        try:
            if (not self.queryspace):
                fEng = FirethornSetupEngine()
                self.queryspace = fEng.create_query_schema(self.resource)
        except Exception as e:
            logging.exception(e)   
             
        return Query(query, self.queryspace)


if __name__ == "__main__":
    ft = Firethorn()
    print(ft.get_public_workspaces())
    osa = ft.get_workspace("OSA")
    myquery = osa.new_query("SELECT * FROM ATLASDR1.Filter")
    myquery.run(mode="SYNC")
    print (myquery.status())
    print (myquery.get_error())
    print (myquery.results().as_astropy())
    print (myquery)
    print (myquery.results())

    #print (osa.get_tables(schema="TAP_SCHEMA"))
    #print (osa.get_columns(table="TAP_SCHEMA.tables"))
    #table = osa.query("SELECT TOP 10 table_name as tname from TAP_SCHEMA.tables")
    #print (table.get_table())

