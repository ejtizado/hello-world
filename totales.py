

import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

# -----
from pcs import *

YEAR = 2015
COUNTRY = 'BRAZIL'

# limit is set for testing purposes, to load full datasets, set to None
limit = None

# 1. --- Exportation from logistic hub
print "Extracting HUBS data..."
chain = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )

cstates = [node for (node,) in chain.get_nodes(1)]
export = {state.main_id: chain.get_subchain(state).get_sum() for state in cstates}


# 2. --- Production from municipality source
print "Extracting MUNIC data..."
munic = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )

mstates = [node for (node,) in munic.get_nodes(1)]
source = {state.main_id: munic.get_subchain(state).get_sum() for state in mstates}


total_export = 0
total_source = 0

state_id = sorted(set(source.keys() + export.keys()))
print "STATE,MUNIC,HUBS"
for sid in state_id:
    dst = export[sid] if sid in export else 0
    src = source[sid] if sid in source else 0
    print str(sid) + "," + str(src) + "," + str(dst)
    total_export += dst
    total_source += src
print "--------------"
print "Total export = " + str(total_export)
print "Total source = " + str(total_source)
