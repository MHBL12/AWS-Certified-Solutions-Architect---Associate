import boto3
import json
import os
import sys 
import netCDF4
from  itertools  import groupby
from operator import attrgetter
from pathlib import Path
from dateutil.parser import parse
from datetime import datetime
import uuid
import xarray as xr

filename = ''
dateFilter = 'prods_op_mogreps-uk_20140101'
source_bucket = 'mogreps-uk'
local_dir = '/mnt/'
output_bucket = 'mogreps-uk-json'


if not os.path.exists(local_dir):
   os.mkdir(local_dir)
#end_if

#Get the latest .nc file of MOGREPS S3 bucket using boto3

s3 = boto3.resource('s3',
                     use_ssl=False,
                     region_name="eu-west-2")
bucket = s3.Bucket(source_bucket)
unsorted = []
for file in bucket.objects.filter(Prefix=dateFilter):
   unsorted.append(file.key)
   print "Retriving " , file

if len(unsorted) == 0:
   print ".nc file not found in MOGREPS S3.  Exit the program"
   exit(1)

# download the latest .nc file to EC2 
filename = max(unsorted)
file_in = local_dir + filename
print "Copy .nc file to EC2 " , file_in
bucket.download_file(filename, file_in)
  
# Open file in a netCDF reader
nc = netCDF4.Dataset(file_in, 'r')

#Look at the variables
#print(nc.variables.keys())

#Look at the dimensions
#print(nc.dimensions)

#Look at interesting variables
interesting_variables = {vname: v for vname, v in nc.variables.items() if 'grid_mapping' in v.ncattrs()}
#for name, var in interesting_variables.items():
#    print(name)
#    print(var)


groupby(interesting_variables.values(), attrgetter('dimensions'))

groups = []
uniquekeys = []
data = interesting_variables.values()
keyfunc = attrgetter('dimensions')
data = sorted(data, key=keyfunc)
for k, g in groupby(data, keyfunc):
   groups.append(list(g))      # Store group iterator as a list
   uniquekeys.append(k)

#print(uniquekeys)
#print(groups)

group_names = [[var.name for var in group] for group in groups]
list(zip(uniquekeys, group_names))

def generate_products(nco, dims, variables):
    prod_name = '_'.join(v.name for v in variables)
    return {
        'name': prod_name,
        'description': prod_name,
        'metadata_type': 'eo',
        'metadata': {
            'platform': {'code': 'mogreps'},
            'product_type': prod_name,
            'format': {
                'name': 'NETCDF'
            }
        },
        'measurements': [
            {'name': v.name,
             'dtype': str(v.dtype),
             'units': str(v.units),
             'nodata': v._FillValue}
            for v in variables
        ]
    }

prods = [generate_products(nc, keys, variables)
            for keys, variables in zip(uniquekeys, groups)]
#print(prods)


def find_bounds(filename, dims):
    bounds = {}
    with xr.open_dataset(filename) as ds:
        for dim in dims:
            coord = ds[dim]
            if coord.axis == 'X':
                bounds['left'] = float(coord.min())
                bounds['right'] = float(coord.max())
            elif coord.axis == 'Y':
                bounds['top'] = float(coord.max())
                bounds['bottom'] = float(coord.min())
            elif coord.axis == 'T':
                bounds['start'] = coord.min().data
                bounds['end'] = coord.max().data
    return bounds

bounds = find_bounds(file_in, ('time', 'grid_latitude', 'grid_longitude'))

#print(bounds)

def make_dataset(filename, dims, variables):
    bounds = find_bounds(filename, dims)

    p = Path(filename)

    mtime = datetime.fromtimestamp(p.stat().st_mtime)
    return {
        'id': str(uuid.uuid4()),
        'processing_level': 'modelled',
        'product_type': 'gamma_ray',
        'creation_dt': mtime.isoformat(),
        'extent': {
            'coord': {
                'ul': {'lon': bounds['left'], 'lat': bounds['top']},
                'ur': {'lon': bounds['right'], 'lat': bounds['top']},
                'll': {'lon': bounds['left'], 'lat': bounds['bottom']},
                'lr': {'lon': bounds['right'], 'lat': bounds['bottom']},
            },
            'from_dt': str(bounds['start']),
            'to_dt': str(bounds['end']),
        },
        'format': {'name': 'NETCDF'},
        'image': {
            'bands': {
                vname: {
                    'path': filename,
                    'layername': vname,
                } for vname in variables
            }
        },
        'lineage': {'source_datasets': {}},
    }

odc_ds = make_dataset(file_in, uniquekeys[1], group_names[1])

#serialize and write to json file in s3
output_key = filename.replace('.nc', '.json')
output_path = "s3://" + output_bucket + '/' + output_key
print "Write transformed data to ", output_path

#write data to an S3 object using boto3
obj = s3.Object(output_bucket,output_key)
obj.put(Body=json.dumps(odc_ds))



def find_interesting_vars(nco):
    interesting_variables = {vname: v
                             for vname, v in nco.variables.items()
                             if 'grid_mapping' in v.ncattrs()}
    groups = {}
    data = interesting_variables.values()
    get_dimensions = attrgetter('dimensions')
    data = sorted(data, key=get_dimensions)
    for k, g in groupby(data, get_dimensions):
        groups[k] = list(g)  # Store group iterator as a list
 
    return groups


def disp_time(name):
    print(len(nc.variables[name][:]), nco.variables[name][:])





# end_for


