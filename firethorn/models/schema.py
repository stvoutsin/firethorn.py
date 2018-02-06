from core.firethorn_engine import FirethornEngine

class Schema(object):
    """Schema class, equivalent to a Firethorn ADQL Schema
    
    Attributes
    ----------
    ident: string, optional
        The Identity URL of the schema
        
    name: string, optional
        The Name of the Schema
        
    resource: string, optional
        The URL of the parent resource    
    """
        
    
    def __init__(self, ident=None, name=None, resource=None):
        self.ident = ident        
        self.name = name             
        self.firethorn_engine = FirethornEngine()
        return        


    @property
    def ident(self):
        return self.__ident
        
        
    @ident.setter
    def ident(self, ident):
        self.__ident = ident


    @property
    def name(self):
        return self.__name
        
        
    @name.setter
    def name(self, name):
        self.__name = name


    @property
    def resource(self):
        return self.__resource
        
        
    @resource.setter
    def queryspace(self, resource):
        self.__resource = resource
