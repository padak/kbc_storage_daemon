"""
This is a setup.py script for creating a Python package.
"""

from setuptools import setup, find_packages

setup(
    name='KeboolaStorageDaemon',
    version='1.0.0',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'keboola-storage-daemon=daemon.main:main',  # Adjust the callable as needed
        ]
    },
    # Optionally include data files if they are required by your package
    data_files=[
        ('.', ['.env.template']), 
        ('docs', ['docs/production_design.md']),
    ],
) 