import iris
import netCDF4
import numpy.ma as ma
import numpy as np
import six
from iris.fileformats.netcdf import NetCDFDataProxy
from iris._lazy_data import as_lazy_data
import pandas as pd
import boto3
import tempfile
import urllib.request
import os

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


def open_as_local(path, storage_options=None):
    if path.startswith('s3://'):
        bucket, key = path[len('s3://'):].split('/', 1)
        s3 = boto3.resource('s3')

        if storage_options and storage_options.get('anon', False):
            s3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)

        try:
            object_body = s3.Bucket(bucket).Object(key).get()['Body'].read()
        except s3.meta.client.exceptions.NoSuchKey:
            raise IOError(f'No such file {path}')

        file = tempfile.NamedTemporaryFile()
        file.write(object_body)
        file.seek(0)
        return file

    if path.startswith('http://') or path.startswith('https://'):
        object_body = urllib.request.urlopen(path).read()
        file = tempfile.NamedTemporaryFile()
        file.write(object_body)
        file.seek(0)

        return file

    return open(path, 'rb')


class CheckingNetCDFDataProxy(NetCDFDataProxy):
    """A reference to the data payload of a single NetCDF file variable."""

    __slots__ = ('shape', 'dtype', 'path', 'variable_name', 'fill_value',
                 'safety_check_done', 'fatal_fail', 'local_file', '_tempfile', 'storage_options')

    def __init__(self, shape, dtype, path, variable_name,
                 fill_value=None, do_safety_check=False, storage_options=None):
        self.safety_check_done = do_safety_check
        self.shape = shape
        self.dtype = dtype
        self.path = path
        self.variable_name = variable_name
        self.fill_value = fill_value
        self.fatal_fail = False
        self.local_file = None
        self._tempfile = None
        self.storage_options = storage_options

    @property
    def ndim(self):
        return len(self.shape)

    def ensure_local_exists(self):
        if (not os.path.exists(self.local_file.name)) and (not self.local_file.name == self.path):
            try:
                self.local_file.close()
            except FileNotFoundError:
                pass
            self.local_file = open_as_local(self.path)

    def check(self):

        try:
            self.local_file = open_as_local(self.path)
        except IOError:
            self.fatal_fail = "no such file %s" % self.path
            self.safety_check_done = True
            return

        try:
            # TODO: Pass in named temp file instead
            dataset = netCDF4.Dataset(self.local_file.name)
        except (OSError, IOError):
            self.fatal_fail = f"Could not no read file {self.local_file.name} source ({self.path})"
            self.safety_check_done = True
            return

        try:
            variable = dataset.variables[self.variable_name]
        except KeyError:
            self.fatal_fail = f"no variable {self.variable_name} in file {self.local_file.name} (source {self.path})"
            self.safety_check_done = True
            return

        if variable.shape != self.shape:
            self.fatal_fail = f"Shape of data {variable.shape} doesn't match expected {self.shape}"
            self.safety_check_done = True
            return

        # TODO check variables???
        self.safety_check_done = True

    def _null_data(self, keys):
        return ma.masked_all(self.shape)[keys]

    def __getitem__(self, keys):
        if not self.safety_check_done:
            self.check()

        if self.fatal_fail:
            return self._null_data(keys)

        self.ensure_local_exists()

        try:
            dataset = netCDF4.Dataset(self.local_file.name)
            variable = dataset.variables[self.variable_name]
            # Get the NetCDF variable data and slice.
            var = variable[keys]
        except RuntimeError:
            # TODO: Maybe raise a warning or handle this somehow.
            self.fatal_fail = "Got exception when accessing the file with netCDF4"
            return self._null_data(keys)
        finally:
            if dataset:
                dataset.close()
        return np.asanyarray(var)

    def __repr__(self):
        fmt = '<{self.__class__.__name__} shape={self.shape}' \
              ' dtype={self.dtype!r} path={self.path!r}' \
              ' variable_name={self.variable_name!r}>'
        return fmt.format(self=self)

    def __getstate__(self):
        return {attr: getattr(self, attr) for attr in self.__slots__}

    def __setstate__(self, state):
        for key, value in six.iteritems(state):
            setattr(self, key, value)


def create_syntheticube(template_cube, object_uri,
                        replacement_coords, fill_value=1e20, storage_options=None):
    cnp = CheckingNetCDFDataProxy(
        shape=template_cube.shape,
        dtype=template_cube.dtype,
        path=object_uri,
        variable_name=template_cube.var_name,
        fill_value=fill_value,
        storage_options=storage_options)
    new_mdata = as_lazy_data(cnp)

    syntheticube = template_cube.copy(data=new_mdata)

    for coord_name, coord_value in replacement_coords.items():
        if coord_name == 'forecast_reference_time':
            coord_value = pd.to_datetime(coord_value)
        try:
            syntheticube.coord(coord_name).points = coord_value
        except ValueError:
            if isinstance(coord_value, str):
                syntheticube.coord(coord_name).points = [float(x) for x in coord_value.split()]
            else:
                syntheticube.coord(coord_name).points = syntheticube.coord(coord_name).units.date2num(coord_value)
        syntheticube.coord(coord_name).bounds = None

    return syntheticube


def load_hypotheticube(template_cube_path, var_name,
                       replacement_coords, object_uris, storage_options=None):
    file = None
    try:
        file = open_as_local(template_cube_path)
        template_cube = iris.load_cube(file.name, var_name)
    finally:
        if file:
            file.close()

    cubes = iris.cube.CubeList([])
    for index, replacement_coord in replacement_coords.iterrows():
        cubes.append(
            create_syntheticube(template_cube,
                                object_uris[index],
                                replacement_coord, storage_options=storage_options))

    hypotheticube = cubes.merge().concatenate_cube()
    hypotheticube.remove_coord("time")

    return hypotheticube
