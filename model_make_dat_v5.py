#!/bin/bash/env python

import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

# -----
from pcs import *
from collections import defaultdict

YEAR = 2015
COUNTRY = 'BRAZIL'
limit = None

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

# --- Checking if optimization is possible
hsum = 0
for i in hubs: hsum += hubs[i]

msum = 0
for i in muns: msum += muns[i]

print "Total in logistic hubs : %d tons" % hsum
print "Total in municipalities: %d tons" % msum
print "------ difference -----: %d tons" % (hsum - msum)

if hsum > msum:
    print "It can not fill the logistic hubs with this municipal production!"
else:
    print "Perfect! This has enough municipal production to fill the logistic hubs"
    # --- GLPK optimization
    nodes = max( max(muns.keys()), max(hubs.keys()) )
    print "Writing DAT file ... "
    fout = open("../../lp_2017/lp_muns_hubs.dat", 'w')  # <<<< /PATH/TO/lp_muns_hubs.dat
    fout.write("data;\n\n")
    fout.write("param n := " + str(nodes) + ";\n\n")
    fout.write("param : muns : src :=\n")
    for idm in muns:
        fout.write("  " + str(idm) + " " + str(int(muns[idm]+0.5)) + "\n")

    fout.write("  ;\n\nparam : hubs : dst :=\n")
    for idh in hubs:
        fout.write("  " + str(idh) + " " + str(int(hubs[idh]+0.5)) + "\n")

    fout.write("  ;\n\nparam : routes : cost :=\n")
    for (idm,idh,value) in cost:
        fout.write("  " + str(idm) + " " + str(idh) + " " + str(int(value+0.5)) + "\n")

    fout.write("  ;\n\nend;\n")
    fout.close()

    # import os
    # os.sys("/PATH/TO/glpsol --simplex -m /PATH/TO/lp_muns_hubs.mod -d /PATH/TO/lp_muns_hubs.dat")
    # process GLPK_MUNS_HUBS.txt (valid as CSV)
