import snappy
from snappy import GPF
from snappy import jpy
from snappy import ProductIO


manifest_path = "/home/cullens/Development/s2d2/temp/S1B_IW_GRDH_1SDV_20180504T001446_20180504T001511_010764_013ABB_0FBB.SAFE_FINAL.dim"
dataproduct_r = snappy.ProductIO.readProduct(manifest_path)


print(dataproduct_r.getBandNames())

for band in dataproduct_r.getBandNames():
    print(band)