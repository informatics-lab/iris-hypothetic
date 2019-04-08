import iris
import netCDF4
import numpy.ma as ma
import numpy as np
import six
from iris.fileformats.netcdf import NetCDFDataProxy
from iris._lazy_data import as_lazy_data
import pandas as pd


from ._version import get_versions
__version__ = get_versions()['version']
del get_versions


class CheckingNetCDFDataProxy(NetCDFDataProxy):
    """A reference to the data payload of a single NetCDF file variable."""

    __slots__ = ('shape', 'dtype', 'path', 'variable_name', 'fill_value',
                 'safety_check_done', 'fatal_fail')

    def __init__(self, shape, dtype, path, variable_name,
                 fill_value=None, do_safety_check=False):
        self.safety_check_done = do_safety_check
        self.shape = shape
        self.dtype = dtype
        self.path = path
        self.variable_name = variable_name
        self.fill_value = fill_value
        self.fatal_fail = False

    @property
    def ndim(self):
        return len(self.shape)

    def check(self):
        # TODO: Make named temporaty file
        # TODO: Read path into temp file
        try:
            # TODO: Pass in named temp file instead
            dataset = netCDF4.Dataset(self.path)
        except OSError:
            self.fatal_fail = "no such file %s" % self.path
            self.safety_check_done = True
            return

        try:
            variable = dataset.variables[self.variable_name]
        except KeyError:
            self.fatal_fail = "no variable {} in file {}".format(
                self.variable_name, self.path)
            self.safety_check_done = True
            return

        if variable.shape != self.shape:
            self.fatal_fail = "Shape of data {} doesn't match expected {}"
            self.fatal_fail = self.fatal_fail.format(variable.shape,
                                                     self.shape)
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

        try:
            dataset = netCDF4.Dataset(self.path)
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
                        replacement_coords, fill_value=1e20):
    cnp = CheckingNetCDFDataProxy(
        shape=template_cube.shape,
        dtype=template_cube.dtype,
        path=object_uri,
        variable_name=template_cube.var_name,
        fill_value=fill_value)
    new_mdata = as_lazy_data(cnp)

    syntheticube = template_cube.copy(data=new_mdata)

    for coord_name, coord_value in replacement_coords.items():
        if coord_name == 'forecast_reference_time':
            coord_value = pd.to_datetime(coord_value)
        try:
            syntheticube.coord(coord_name).points = coord_value
        except ValueError:
            coord = syntheticube.coord(coord_name)
            num = coord.units.date2num(coord_value)
            syntheticube.coord(coord_name).points = num
        syntheticube.coord(coord_name).bounds = None

    return syntheticube


def load_hypotheticube(template_cube_path, var_name,
                       replacement_coords, object_uris):
    template_cube = iris.load_cube(template_cube_path, var_name)
    cubes = iris.cube.CubeList([])
    for index, replacement_coord in replacement_coords.iterrows():
        cubes.append(
            create_syntheticube(template_cube,
                                object_uris[index],
                                replacement_coord))

    hypotheticube = cubes.merge_cube()
    hypotheticube.remove_coord("time")

    return hypotheticube
