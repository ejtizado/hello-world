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
optim = "glpk"

filedat = "../../lp_2017/lp_muns_hubs_v5.dat"

# ---------------------------------------------------------
# --- Exportation from logistic hub and from state
print "Extracting CHAIN data..."
chain = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )
# >>>
hubs = defaultdict(float)
st_hubs = defaultdict(float)
for flow in chain.flows:
    st_hubs[ flow.path[1].main_id ] += flow.raw_vol
    if flow.path[2].name[:7] != 'UNKNOWN':
        hubs[ flow.path[2].main_id ] += flow.raw_vol

# ---------------------------------------------------------
# --- Production from municipality source and from state
print "Extracting MUNIC data..."
munic = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
# >>>
st_muns = defaultdict(float)
for flow in munic.flows:
    st_muns[ flow.path[1].main_id ] += flow.raw_vol

rat = {}
for idm in st_muns:
    rat[idm] = st_hubs[idm] / st_muns[idm]
    if rat[idm] > 1.0: rat[idm] = 1.0

muns = defaultdict(float)
for flow in munic.flows:
    muns[ flow.path[0].main_id ] += flow.raw_vol * rat[flow.path[1].main_id]

# ---------------------------------------------------------
# --- Cost list
print "Extracting COST data..."
cost = get_cost_table( get_country_id(COUNTRY), muns.keys(), hubs.keys(), YEAR, mode='list' )

# --- Check
hsum = 0
for i in hubs: hsum += hubs[i]
msum = 0
for i in muns: msum += muns[i]

print "Total in logistic hubs : %d tons\n" % hsum
print "Total in municipalities: %d tons\n" % msum
print "-----------------------: %d tons\n" % (hsum - msum)
if hsum > msum:
    print "Flow no valid!"
    stop

# ---------------------------------------------------------
# --- Optimization
if optim == "glpk":
    # GLPK optimization
    nodes = max( max(muns.keys()), max(hubs.keys()) )
    print "Writing DAT file ... "
    fout = open(filedat, 'w')
    fout.write("data;\n")
    fout.write("# with ratio = all_hubs / production\n")
    fout.write("# with ratio limited to 1.0\n\n")
    fout.write("param n := " + str(nodes) + ";\n\n")
    fout.write("param : muns : src :=\n")
    for idm in muns: fout.write("  " + str(idm) + " " + str(int(muns[idm]+0.5)) + "\n")
    fout.write("  ;\n\nparam : hubs : dst :=\n")
    for idh in hubs: fout.write("  " + str(idh) + " " + str(int(hubs[idh]+0.5)) + "\n")
    fout.write("  ;\n\nparam : routes : cost :=\n")
    for (idm,idh,value) in cost: fout.write("  " + str(idm) + " " + str(idh) + " " + str(int(value+0.5)) + "\n")
    fout.write("  ;\n\nend;\n")
    fout.close()

    # os.sys("/PATH/TO/glpsol --simplex -m /PATH/TO/lp_muns_hubs.mod -d lp_muns_hubs.dat")
else:
    # PYTHON optimization
    chk = [idh for (idm, idh, val) in cost if idm == idh ]
    for idh in hubs:
        if idh not in chk and idh in muns:
            cost.append( (idh,idh,0.0) )

    best = defaultdict( lambda : defaultdict(float) )
    cost.sort( key=lambda x: x[2])
    n_hubs = len(hubs)
    for (idm, idh, val) in cost:
        buy = hubs[idh]
        sell = muns[idm]
        if buy == 0 or sell == 0: continue
        trade = min(sell, buy)
        best[idm][idh] += trade
        muns[idm] -= trade
        hubs[idh] -= trade
        if hubs[idh] == 0:
            n_hubs -= 1
            if n_hubs == 0: break

fout = open("PYTHON_MUNS_HUBS.txt", 'w')
fout.write("MUN_ID,HUB_ID,RAW_VOL\n")
for idm in best:
    for idh in best[idm]:
        fout.write(str(idm) + ',' + str(idh) + ',' + str(int(best[idm][idh]+0.5)) + "\n")

fout.close()
