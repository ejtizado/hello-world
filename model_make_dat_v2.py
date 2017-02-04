
import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

# -----
from collections import defaultdict

def consolidate_list2( lst ):
    rst = defaultdict(int)
    for (k,v) in lst: 
        rst[k] += v
    return rst

def consolidate_list3( lst ):
    ls1 = defaultdict(int)
    ls2 = defaultdict(int)
    for (k,m,v) in lst:
        ls1[k] += v
        ls2[m] += v
    return ls1,ls2

# -----
from pcs import *

YEAR = 2015
COUNTRY = 'BRAZIL'

# limit is set for testing purposes, to load full datasets, set to None
limit = None

# --- Exportation from logistic hub and from state
print "Extracting HUBS data..."
data = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )
# remove municipality 2 from path
data.chop(3)
# remove biome from path
data.chop(0)
# remove others
data.chop(2)
data.chop(2)
data.chop(2)
# merge flows with similar paths and attributes
data.consolidate()
# reinitialize object to update __repr__
data = SupplyChain(data.flows)
# >>>
data = [ (flow.path[1].main_id, flow.path[0].main_id, flow.raw_vol) for flow in data.flows if flow.path[1].name[:7] != 'UNKNOWN' ]
chain, hstate = consolidate_list3(data)
cols_id = sorted(chain.keys())

# --- Production from municipality source and from state
print "Extracting MUNIC data..."
data = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
# >>>
fstate = [ (flow.path[1].main_id, flow.raw_vol) for flow in data.flows ]
fstate = consolidate_list2(fstate)

# --- Ratio
ratio = {}
for mid in fstate.keys():
    if mid not in hstate: hstate[mid] = 0
    ratio[mid] = hstate[mid] / fstate[mid]

munic = [ (flow.path[0].main_id, flow.raw_vol * ratio[flow.path[1].main_id]) for flow in data.flows ]
munic = consolidate_list2(munic)
rows_id = sorted(munic.keys())

fout = open("../state_ratio.txt",'w')
fout.write("STATE,RATIO\n")
for key in ratio:
    fout.write(str(key) + "," + str(ratio[key]) + "\n")
fout.close()

# --- Cost list
print "Extracting COST data..."
nation_id = get_country_id(COUNTRY)
cost = get_cost_list(nation_id, rows_id, cols_id, YEAR, mode='list')

# --- Make the data files for GLPK Model
nodes = max( max(rows_id), max(cols_id) )
print "Writing data..."
fout = open("../../lp_2017/lp_hubs_ratio.dat", 'w')
fout.write("data;\n\n")
fout.write("param n := " + str(nodes) + ";\n\n")
fout.write("param : muns : src :=\n")
for idm in munic:
    fout.write("  " + str(idm) + " " + str(int(munic[idm]+0.5)) + "\n")
fout.write("  ;\n\n")
fout.write("param : hubs : dst :=\n")
for idh in chain:
    fout.write("  " + str(idh) + " " + str(int(chain[idh]+0.5)) + "\n")
fout.write("  ;\n\n")
fout.write("param : routes : cost :=\n")
for (idm,idh,value) in cost:
    fout.write("  " + str(idm) + " " + str(idh) + " " + str(int(value)) + "\n")
fout.write("  ;\n\n")
fout.write("end;\n")
fout.close()

#os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/lp_2017/")
#os.sys("./glpsol -m lp_hubs_O2.mod -d lp_hubs.dat -o lp_hubs.log")
