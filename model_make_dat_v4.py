#!/bin/bash/env python

from collections import defaultdict

def consolidate_last( lst ):
    n = len(lst[0]) - 1
    rst = [ defaultdict(float) for i in range(n) ]
    for item in lst:
        for i in range(n):
            rst[i][ item[i] ] += item[n]
    return rst if n > 1 else rst[0]

# -----
from pcs import *

YEAR = 2015
COUNTRY = 'BRAZIL'
limit = None

filedat = "../../lp_2017/lp_hubs_v4.dat"
comment ="\n# without Amazonas state (id: 385)\n# with ratio = all_hubs / production\n# with limit ratio to 1.0\n\n"
# SIN AMAZONAS ##
print("WARNING: WITHOUT AMAZONAS IN LOGISTUC HUBS >>> %s" % filedat)

# ---------------------------------------------------------
# --- Exportation from logistic hub and from state
print "Extracting HUBS data..."
chain = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )
## --- ALL HUBS WITHOUT STATE AMAZONAS (ID: 385)
st_hall = [ (flow.path[1].main_id, flow.raw_vol) for flow in chain.flows if flow.path[1].main_id != 385 ]
st_hall = consolidate_last(st_hall)
# >>>
hdata = [ (flow.path[1].main_id, flow.path[2].main_id, flow.raw_vol) for flow in chain.flows if flow.path[2].name[:7] != 'UNKNOWN' and flow.path[1].main_id != 385 ]
st_hubs, hubs = consolidate_last(hdata)
# >>>
cols_id = sorted(hubs.keys())
# ---------------------------------------------------------
# --- Production from municipality source and from state
print "Extracting MUNIC data..."
munic = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
# >>>
st_muns = [ (flow.path[1].main_id, flow.raw_vol) for flow in munic.flows ]
st_muns = consolidate_last(st_muns)
rat = {}
for idm in st_muns:
    if idm not in st_hall: st_hall[idm] = 0 # ??? innecesario es defaultdict <<<<<<<<<<<<<<<<<<
    rat[idm] = st_hall[idm] / st_muns[idm]
    if rat[idm] > 1.0: rat[idm] = 1.0
# >>>
mdata = [ (flow.path[0].main_id, flow.raw_vol * rat[flow.path[1].main_id]) for flow in munic.flows ]
muns = consolidate_last(mdata)
# >>>
rows_id = sorted(muns.keys())
# ---------------------------------------------------------
# --- Cost list
print "Extracting COST data..."
nation_id = get_country_id(COUNTRY)
cost = get_cost_table(nation_id, rows_id, cols_id, YEAR, mode='list')
# ---------------------------------------------------------
# --- Make the dat file to run GLPK model
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
#os.sys("./glpsol --simplex -m lp_hubs.mod -d " + filedat + " -o lp_hubs.log")

# --- Make python optimization

>>>> anadir los costes de idm a idm y de idh a idh como 0.0



cost.sort( key=lambda x: x[2])
ttrade = 0
optim = []
n_hubs = len(hubs)
for (idm, idh, val) in cost:
    buy = hubs[idh]
    sell = muns[idm]
    if buy == 0 or sell == 0: continue
    trade = min(sell, buy)
    ttrade += trade
    optim.append( (idm, idh, trade) )
    muns[idm] -= trade
    hubs[idh] -= trade
    if hubs[idh] == 0:
        n_hubs -= 1
        if n_hubs == 0: break

xxx = defaultdict( lambda : defaultdict(float) )
for (idm, idh, trade) in optim: xxx[idm][idh] += trade
