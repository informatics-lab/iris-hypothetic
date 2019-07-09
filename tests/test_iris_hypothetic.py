# -*- coding: utf-8 -*-
import pytest


def test_import():
    from iris_hypothetic import load_hypotheticube


def teat_pickle():
    from iris_hypothetic import CheckingNetCDFDataProxy
    import pickle
    import numpy as np

    shape = (3, 970, 1042)
    dtype = 'float32'
    path = "s3://informatics-webimages/0002093217dad7e7c21011f13bff4b1eac270f6a.nc"
    var_name = 'cloud_base_altitude_assuming_only_consider_cloud_area_fraction_greater_than_4p5_oktas'

    p = CheckingNetCDFDataProxy(shape, dtype, path, var_name)
    before = p[0, 0]
    pickled = pickle.dumps(p)
    after = pickle.loads(pickled)[0, 0]
    np.testing.assert_array_equal(before, after)
