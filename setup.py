# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='iris_hypothetic',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Hypotheticube loader for iris',
    url='https://github.com/informatics-lab/iris-hypothetic',
    maintainer='Jacob Tomlinson',
    maintainer_email='jacob.tomlinson@informaticslab.co.uk',
    license='BSD',
    py_modules=['iris_hypothetic'],
    packages=find_packages(),
    package_data={'': ['*.csv', '*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False, )
