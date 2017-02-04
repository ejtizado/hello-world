
import os
os.chdir("/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/")

import sys
sys.path += ['/home/jorge/mounts/usb1/Research/proyectos/Sweden/TRASE/PCS/PYTHON_PACKAGES/']

# -----
from collections import defaultdict, deque

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

# 1. --- Exportation from logistic hub and from state
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
data.consolidate()
# reinitialize object to update __repr__
data = SupplyChain(data.flows)
# ---
data = [ (flow.path[1].main_id, flow.path[0].main_id, flow.raw_vol) for flow in data.flows if flow.path[1].name[:7] != 'UNKNOWN' ]
chain, hstate = consolidate_list3(data)
cols_id = sorted(chain.keys())

# 2. --- Production from municipality source and from state
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
cost = get_cost_list(nation_id, rows_id, cols_id, YEAR, mode='list') # posible order by en SQL, to optim in python

# check the munic that are hubs and they are cost value
munic_ok = []
for (idm,idh,c) in cost:
    if idm == idh: munic_ok.append(idm)

cost = deque(cost)
for i in set(rows_id).difference(munic_ok):
    cost.appendleft( (i,i,0) )
   

# 4. --- Make optimization
cost.sort( key=lambda x: x[2])
n_hubs = len(chain)
for (idm,idh,val) in cost:
    buy = chain[idh]
    if buy == 0: continue
    sell = munic[idm]
    trade = sell if sell < buy else buy
    print (idh,idm,trade)
    munic[idm] -= trade
    chain[idh] -= trade
    if chain[idh] == 0:
        n_hubs -= 1
        if n_hubs == 0: break


print "Remaining hubs...
for k in chain:
    if chain[k] > 0: print k
print "---"
        

#==============================================================================
# for (idm,idh,val) in cost:
#     buy = chain[idh]
#      if buy <= 0: continue
#      sell = munic[idm]
#      if sell >= buy:
#          print (idh,idh,buy)
#          chain[idh] = 0
#          munic[idm] -= buy
#          n_hubs -= 1
#          if n_hubs == 0: break
#      else:
#          print (idh,idm,sell)
#          chain[idh] -= sell
#          munic[idm] = 0
#==============================================================================

            
            
        
        
    



