
import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

# -----
from pcs import *

YEAR = 2015
COUNTRY = 'BRAZIL'

# limit is set for testing purposes, to load full datasets, set to None
limit = 50


def farm_source(limit):
    """farm source production"""
    data = get_data( get_ref_id('BRAZIL MUNICIPAL SOY PRODUCTION 2015'), limit=limit )
    # ids = [ node.main_id for (node,) in data.get_nodes(0)]
    # ton = [ flow.vols for flow in data.flows]
    return [ (flow.path[0].main_id, flow.vols[0]) for flow in data.flows ]



def lhubs_store(limit):
    """logistic hub exportation"""
    chain = load_supply_chain( get_ref_id('COP before indicators'), limit=limit )
    # remove municipality from path
    chain.chop(3)
    # remove biome from path
    chain.chop(0)
    # remove state from path
    chain.chop(0)
    # merge flows with similar paths and attributes
    chain.consolidate()
    # rename node types
    for flow in chain.flows:
        flow.path[0].type = 'LOGISTICS HUB'
        flow.path[1].type = 'EXPORTER'
        flow.path[3].type = 'IMPORTER'
    # reinitialize object to update __repr__
    chain = SupplyChain(chain.flows)
    return [ (flow.path[0].main_id, flow.vols[0]) for flow in chain.flows if flow.path[0].name[:7] != 'UNKNOWN' ]



# ---------------------------------------------------------
munic = farm_source(limit)
chain = lhubs_store(limit)

rows_id = sorted( set([code for (code,vols) in munic]) )
cols_id = sorted( set([code for (code,vols) in chain]) )

nation_id = get_country_id(COUNTRY)
cost = get_cost_table(nation_id, rows_id, cols_id, YEAR)

# --- Make the data file for GLPK Model
fout = open("../../lp_2017/lp_hubs_O1.dat", 'w')
fout.write("data;\n")
fout.write("param n := 4;\n")
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
fout.write("  ;\n\n")
fout.write("end;\n")
fout.close()

os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/lp_2017/")
os.sys("./glpsol --model lp_hubs_O1.mod --data lp_hubs_O1.dat")

# ----- Alternative -----
# rows = dict(munic)
# rows_id = sorted( rows.keys() )
#
# cols = dict(chain)
# cols_id = sorted( cols.keys() )
