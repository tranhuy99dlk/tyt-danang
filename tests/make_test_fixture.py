# -*- coding: utf-8 -*-
"""Tao du lieu GIA LAP (khong phai du lieu that) de chay thu pipeline. Chi dung trong thu muc kiem thu."""
import json, sys, zipfile
from pathlib import Path
import numpy as np, pandas as pd, rasterio
from rasterio.transform import from_origin
root = Path(sys.argv[1]); raw = root/"data/raw"
rng = np.random.default_rng(0)
ref = pd.read_csv(root/"data/reference/danh_sach_56_tram.csv", encoding="utf-8-sig")
W,S,E,N = 107.80,15.92,108.34,16.20
dists = ref.quan_huyen.unique().tolist()
def ring(x0,y0,x1,y1): return [{"lon":x0,"lat":y0},{"lon":x1,"lat":y0},{"lon":x1,"lat":y1},{"lon":x0,"lat":y1},{"lon":x0,"lat":y0}]
els=[]; st=[]; rid=1
dx=(E-W)/len(dists)
for i,d in enumerate(dists):
    x0=W+i*dx; x1=x0+dx
    els.append({"type":"relation","id":rid,"tags":{"admin_level":"6","name":d,"boundary":"administrative"},"members":[{"type":"way","ref":rid,"role":"outer","geometry":ring(x0,S,x1,N)}]}); rid+=1
    cs=ref[ref.quan_huyen==d]; dy=(N-S)/len(cs)
    for j,(_,r) in enumerate(cs.iterrows()):
        y0=S+j*dy; y1=y0+dy
        els.append({"type":"relation","id":rid,"tags":{"admin_level":"8","name":r.ten_day_du,"boundary":"administrative"},"members":[{"type":"way","ref":rid,"role":"outer","geometry":ring(x0,y0,x1,y1)}]}); rid+=1
        if j%9!=4: st.append({"type":"node","id":rid,"lat":y0+dy*rng.uniform(.3,.7),"lon":x0+dx*rng.uniform(.3,.7),"tags":{"name":r.ten_tram,"amenity":"clinic"}})
(raw/"osm").mkdir(parents=True,exist_ok=True)
json.dump({"version":0.6,"osm3s":{"timestamp_osm_base":"FAKE"},"elements":els},open(raw/"osm/admin_2024-12-31.json","w"))
json.dump({"version":0.6,"elements":st},open(raw/"osm/stations_2024-12-31.json","w"))
json.dump({"version":0.6,"elements":st[:5]},open(raw/"osm/stations_current.json","w"))
ways=[]; wid=1; nid=1; nodes={}
xs=np.linspace(W,E,40); ys=np.linspace(S,N,25)
def nidf(x,y):
    global nid
    k=(round(x,6),round(y,6))
    if k not in nodes: nodes[k]=nid; nid+=1
    return nodes[k]
for y in ys:
    ways.append({"type":"way","id":wid,"tags":{"highway":"secondary"},"nodes":[nidf(x,y) for x in xs],"geometry":[{"lon":x,"lat":y} for x in xs]}); wid+=1
for x in xs:
    ways.append({"type":"way","id":wid,"tags":{"highway":"residential"},"nodes":[nidf(x,y) for y in ys],"geometry":[{"lon":x,"lat":y} for y in ys]}); wid+=1
json.dump({"version":0.6,"elements":ways},open(raw/"osm/roads_current.json","w"))
res=0.00083333333; ox=102.14333220476; oy=23.393333575759982
c0=int((107.70-ox)/res); r0=int((oy-16.30)/res); x0=ox+c0*res; y0=oy-r0*res
w=int(0.75/res); h=int(0.50/res)
def wt(p,arr,tr):
    with rasterio.open(p,"w",driver="GTiff",width=arr.shape[1],height=arr.shape[0],count=1,dtype="float32",crs="EPSG:4326",transform=tr,nodata=-99999) as ds: ds.write(arr.astype("float32"),1)
base=rng.lognormal(1,1.2,(h,w)); base[rng.random((h,w))<.6]=-99999
for y,f in ((2015,.85),(2020,.93),(2025,1.0)):
    (raw/"worldpop/pop100m").mkdir(parents=True,exist_ok=True)
    a=np.where(base<0,-99999,base*f); wt(raw/f"worldpop/pop100m/vnm_pop_{y}_CN_100m_R2025A_v1.tif",a,from_origin(x0,y0,res,res))
r1=0.0083333333; w1=int(0.8/r1); h1=int(0.55/r1); tr1=from_origin(107.68,16.32,r1,r1)
k=rng.lognormal(6,1,(h1,w1))
(raw/"worldpop/pop1km").mkdir(parents=True,exist_ok=True); (raw/"worldpop/age1km").mkdir(parents=True,exist_ok=True)
for y in range(2015,2031):
    g=(1+0.015)**(y-2025); wt(raw/f"worldpop/pop1km/vnm_pop_{y}_CN_1km_R2025A_UA_v1.tif",k*g,tr1)
    for b in (60,65,70,75,80,85,90):
        wt(raw/f"worldpop/age1km/vnm_t_{b}_{y}_CN_1km_R2025A_UA_v1.tif",k*g*0.02*(1+0.03*(y-2015)),tr1)
print("fixture ok")
