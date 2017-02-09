
import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

# -----
from collections import defaultdict

def consolidate_last( lst ):
    n = len(lst[0]) - 1
    rst = [ defaultdict(float) for i in range(n) ]
    for item in lst:
        for i in range(n):
            rst[i][ item[i] ] += item[n]
    return rst if n > 1 else rst[0]

def consolidate_last_simple( lst ):
    rst = defaultdict(float)
    for (k,v) in lst:
            rst[k] += v
    return rst

# -----
from pcs import *

YEAR = 2015
COUNTRY = 'BRAZIL'
# limit is set for testing purposes, to load full datasets, set to None
limit = None

filedat = "../../lp_2017/lp_hubs_O3.dat"
comment ="\n# without Amazonas state (id: 385)\n# with limit ratio to 1.0\n\n"
# SIN AMAZONAS ##
print("WARNING: WITHOUT AMAZONAS IN LOGISTUC HUBS >>> %s" % filedat)

# ---------------------------------------------------------
# --- Exportation from logistic hub and from state
print "Extracting HUBS data..."
chain = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )
# >>>
# st_hubs = [node for (node,) in chain.get_nodes(1)]
# st_hubs = {state.main_id: chain.get_subchain(state).get_sum() for state in st_hubs}
#
# hubs = [node for (node,) in chain.get_nodes(2)]
# hubs = {hub.main_id: chain.get_subchain(hub).get_sum() for hub in hubs}
# >>>
# sthd = {s.main_id: s.name for s in [node for (node,) in chain.get_nodes(1)]}
# >>>
## --- ALL HUBS WITHOUT STATE AMAZONAS (ID: 385)
st_hall = [ (flow.path[1].main_id, flow.raw_vol) for flow in chain.flows if flow.path[1].main_id != 385 ]
st_hall = consolidate_last(st_hall)
# >>> real : rdata = [ (flow.path[2].main_id, flow.raw_vol) for flow in chain.flows ]
hdata = [ (flow.path[1].main_id, flow.path[2].main_id, flow.raw_vol) for flow in chain.flows if flow.path[2].name[:7] != 'UNKNOWN' and flow.path[1].main_id != 385 ]
st_hubs, hubs = consolidate_last(hdata)
# >>>
cols_id = sorted(hubs.keys())
# ---------------------------------------------------------
# --- Production from municipality source and from state
print "Extracting MUNIC data..."
munic = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
# >>>
# st_muns = [node for (node,) in munic.get_nodes(1)]
# st_muns = {state.main_id: prods.get_subchain(state).get_sum() for state in st_muns}
# >>> no tendria los datos de state of municipality <<<
# muns = [node for (node,) in munic.get_nodes(0)]
# muns = {mun.main_id: prods.get_subchain(mun).get_sum() for mun in muns}
# >>>
# stmd = {s.main_id: s.name for s in [node for (node,) in munic.get_nodes(1)]}
# >>>
st_muns = [ (flow.path[1].main_id, flow.raw_vol) for flow in munic.flows ]
st_muns = consolidate_last(st_muns)
rat = {}
for idm in st_muns:
    if idm not in st_hall: st_hubs[idm] = 0
    rat[idm] = st_hall[idm]) / st_muns[idm]
    if rat[idm] > 1.0: rat[idm] = 1.0
# >>> real total : xdata = [ (flow.path[0].main_id, flow.raw_vol) for flow in munic.flows ]
mdata = [ (flow.path[0].main_id, flow.raw_vol * rat[flow.path[1].main_id]) for flow in munic.flows ]
muns = consolidate_last(mdata)
# >>>
rows_id = sorted(muns.keys())
# >>>
# fout = open("./state_ratio.txt",'w')
# fout.write("STATE,RATIO\n")
# for key in ratio: fout.write(str(key) + "," + str(ratio[key]) + "\n")
# fout.close()
# ---------------------------------------------------------
# --- Cost list
print "Extracting COST data..."
nation_id = get_country_id(COUNTRY)
cost = get_cost_list(nation_id, rows_id, cols_id, YEAR, mode='list')
# ---------------------------------------------------------
# --- Make the data files to run GLPK Model
nodes = max( max(rows_id), max(cols_id) )
print "Writing data to ... "
fout = open(filedat, 'w')
fout.write("data;\n\n")
fout.write(comment)
fout.write("param n := " + str(nodes) + ";\n\n")
fout.write("param : muns : src :=\n")
for idm in muns:
    fout.write("  " + str(idm) + " " + str(int(muns[idm]+0.5)) + "\n")

fout.write("  ;\n\n")
fout.write("param : hubs : dst :=\n")
for idh in hubs:
    fout.write("  " + str(idh) + " " + str(int(hubs[idh]+0.5)) + "\n")

fout.write("  ;\n\n")
fout.write("param : routes : cost :=\n")
for (idm,idh,value) in cost:
    fout.write("  " + str(idm) + " " + str(idh) + " " + str(int(value+0.5)) + "\n")

fout.write("  ;\n\n")
fout.write("end;\n")
fout.close()

#os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/lp_2017/")
#os.sys("./glpsol -m lp_hubs_O2.mod -d lp_hubs.dat -o lp_hubs.log")
