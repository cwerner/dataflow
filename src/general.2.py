# general
# ======= 
#
# ... gerneal purpose tasks and helper functions

from affine import Affine
import geopandas as gp
import luigi
import math
import numpy as np
import os
from rasterio import features
import xarray as xr

# helper functions ------------------------------------------------

def bbox_to_latlon(bbox, res):
    """Create coords for netcdf file. 
    bbox format: [c1_lon, c1_lat, c2_lon, c2_lat]"""

    lon1, lat1, lon2, lat2 = bbox

    # flip bbox coords if in wrong order
    lat1, lat2 = (lat1, lat2) if lat1 < lat2 else (lat2, lat1)
    lon1, lon2 = (lon1, lon2) if lon1 < lon2 else (lon2, lon1)

    lons = np.arange(lon1 + res/2, lon2, res)
    lats = np.arange(lat1 + res/2, lat2, res) 
    return (lats, lons)

def transform_from_latlon(lat, lon):
    lat = np.asarray(lat)
    lon = np.asarray(lon)
    trans = Affine.translation(lon[0], lat[0])
    scale = Affine.scale(lon[1] - lon[0], lat[1] - lat[0])
    return trans * scale


def calc_area(lat, pixeldegree):
    area_km2 = (110.45 * pixeldegree) * (111.1944 * pixeldegree) * math.cos(lat * (math.pi / 180.0))
    area_ha  = area_km2 * 100
    area_m2  = area_km2 * 1000000

    # calculate gridcell areas THOMAS
    # mean radius of the earth (km)
    radius_Earth = 6367.425
    lat_upper = lat + pixeldegree / 0.5
    lat_lower = lat - pixeldegree / 0.5

    h1 = radius_Earth * math.sin( lat_upper * math.pi / 180.0)
    h2 = radius_Earth * math.sin( lat_lower * math.pi / 180.0)

    area_band   = 2.0 * math.pi * radius_Earth * (h1-h2)  # area of this latitude band
    area_km2_TH = area_band * (pixeldegree / 360.0)

    return area_ha

def add_attributes(da):
    da['lat'].attrs['long_name'] = 'latitude'
    da['lat'].attrs['units'] = 'degrees_north'
    da['lon'].attrs['long_name'] = 'longitude'
    da['lon'].attrs['units'] = 'degrees_east'
    return da

# general purpose tasks ---------------------------------------------

class MakeDirectory(luigi.Task):
    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)

class RasterizeShapefile(luigi.Task):
    shp_file = luigi.Parameter()
    attr     = luigi.Parameter(default="NONE")
    bbox     = luigi.ListParameter()
    res      = luigi.FloatParameter()
    name     = luigi.Parameter(default="variable")
    file_path = luigi.Parameter(default="data.nc")
    
    def run(self):
        fill = np.nan

        # load and potentially reproject shapefile
        shp = gp.read_file(self.shp_file)
        shp = shp.to_crs({'init': 'epsg:4326'})

        # use existing geometry of raster file to use
        lats_, lons_ = bbox_to_latlon(self.bbox, self.res)

        # create cin variable
        data = np.ones((len(lats_), len(lons_)))
        da = xr.DataArray(data, coords=(lats_, lons_), dims=('lat', 'lon'), name=self.name)
        da.attrs['units'] = '-'        

        # create mask or burn attr feature values
        if self.attr == 'NONE':
            shapes = [(feature['geometry'], 1) for feature in shp.iterfeatures()]
        else:
            shapes = ((geom, value) for geom, value in zip(shp.geometry, shp[self.attr]))
            
        raster_data = features.rasterize(shapes, out_shape=data.shape, fill=fill,
                                   transform=transform_from_latlon(da.coords['lat'], da.coords['lon']))

        da = xr.DataArray(raster_data, coords=da.coords, dims=('lat', 'lon'), name=self.name)
        da = add_attributes(da)
        da.to_dataset().to_netcdf(self.output().path, format='NETCDF4_CLASSIC')

    def output(self):
        return luigi.LocalTarget(self.file_path)