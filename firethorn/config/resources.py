'''
Created on Feb 1, 2018

@author: stelios
'''

import os

datahost= os.environ.get('datahost', '')
datadata=os.environ.get('datahost', 'ATLASDR1')
datauser=os.environ.get('datauser', '')
datapass=os.environ.get('datapass', '')
datadriver="net.sourceforge.jtds.jdbc.Driver"
dataurl=os.environ.get('dataurl', '')
default_endpoint=os.environ.get('default_endpoint', '')
osa_endpoint= default_endpoint + "/firethorn/adql/resource/208834"



