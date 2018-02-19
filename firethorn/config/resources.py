'''
Created on Feb 1, 2018

@author: stelios
'''

import os

datahost= os.environ.get('datahost', '')
datadata=os.environ.get('datadata', 'ATLASDR1')
datauser=os.environ.get('datauser', '')
datapass=os.environ.get('datapass', '')
datacatalog=os.environ.get('datacatalog', 'ATLASDR1')
datatype=os.environ.get('datatype', 'mssql')
datadriver="net.sourceforge.jtds.jdbc.Driver"
dataurl=os.environ.get('dataurl', '')
default_endpoint=os.environ.get('default_endpoint', '')
osa_endpoint= os.environ.get('osa_endpoint', '')

