
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



def consolidate_list2( lst ):
    rst = {}
    for (k,v) in lst:
        if k not in rst: rst[k] = 0
        rst[k] += v
    return rst


def consolidate_list3( lst ):
    ls1 = {}
    ls2 = {}
    for (k,m,v) in lst:
        if k not in ls1: ls1[k] = 0
        ls1[k] += v
        if m not in ls2: ls2[m] = 0
        ls2[m] += v
    return ls1,ls2



# 1. --- Exportation from logistic hub
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
# rename node types
#for flow in data.flows:
#    flow.path[1].type = 'LOGISTICS HUB'
# merge flows with similar paths and attributes
data.consolidate()
# reinitialize object to update __repr__
data = SupplyChain(data.flows)
# ---
data = [ (flow.path[1].main_id, flow.path[0].main_id, flow.raw_vol) for flow in data.flows if flow.path[1].name[:7] != 'UNKNOWN' ]
chain, hstate = consolidate_list3(data)
cols_id = sorted(chain.keys())

# 2. --- Production from municipality source
print "Extracting MUNIC data..."
data = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
# ---
fstate = [ (flow.path[1].main_id, flow.raw_vol) for flow in data.flows ]
fstate = consolidate_list2(fstate)

ratio = {}
for mid in fstate.keys():
    if mid not in hstate: hstate[mid] = 0
    ratio[mid] = hstate[mid] / fstate[mid]

munic = [ (flow.path[0].main_id, flow.raw_vol * ratio[flow.path[1].main_id]) for flow in data.flows ]
munic = consolidate_list2(munic)
rows_id = sorted(munic.keys())

# 3. --- Cost table
print "Extracting COST data..."
nation_id = get_country_id(COUNTRY)
cost = get_cost_table(nation_id, rows_id, cols_id, YEAR) # posible order by en SQL, to optim in python


# 4. --- Make the data files for GLPK Model
rows_max = max(rows_id)
cols_max = max(cols_id)
nodes = rows_max if rows_max > cols_max else cols_max

print "Writing data..."
fout = open("../../lp_2017/lp_hubs_ratio.dat", 'w')

fout.write("data;\n\n")

fout.write("param n := " + str(nodes) + ";\n\n")

fout.write("param : muns : src :=\n")
for (idm,ton) in munic:
    fout.write("  " + str(idm) + " " + str(int(ton)) + "\n")
fout.write("  ;\n\n")

fout.write("param : hubs : dst :=\n")
for (idh,ton) in chain:
    fout.write("  " + str(idh) + " " + str(int(ton)) + "\n")
fout.write("  ;\n\n")

fout.write("param : routes : cost :=\n")
for idm in cost.keys():
    for idh in cost[idm].keys():
        fout.write("  " + str(idm) + " " + str(idh) + " " + str(int(cost[idm][idh])) + "\n")
# Que lo haga GLPK
#if (idm in cols_id) and (idm not in cost[idm]):
#   fout.write("  " + str(idm) + " " + str(idm) + " 0\n")
fout.write("  ;\n\n")

fout.write("end;\n")
fout.close()

os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/lp_2017/")
#os.sys("./glpsol -m lp_hubs_O2.mod -d lp_hubs.dat -o lp_hubs.log")
