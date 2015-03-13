#!/usr/bin/python

import setuptools

setup_params = dict(
    name='mettle_protocol',
    version='1.0.0',
    author='Y Team',
    author_email=', '.join([
        'alejandro.rivera@yougov.com',
        'fernando.gutierrez@yougov.com',
        'brent.tubbs@gmail.com',
    ]),
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        'isodate==0.5.0',
        'pika==0.9.14',
        'utc==0.0.3',
    ],
    description='A robust framework for scheduling and executing ETL jobs.',
)

if __name__ == '__main__':
    setuptools.setup(**setup_params)