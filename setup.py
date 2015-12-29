"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
"""
# flake8: noqa
# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))


setup(
    name='dfs_scraper',

    version='0.0.1',

    description='Web Scrapers for sports data',
    long_description='Personal package that contains functions to scrape '
                     'web pages and loads data into database.',

    url='https://github.com/kimjam/dfs_scraper',

    author='James Kim',
    author_email='jamesykim10@gmail.com',

    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 1',

        'Intended Audience :: Developers',
        'Intended Audience :: Sports',
        'Topic :: Sports'

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2.7'
    ],

    keywords='sports scrapers',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    install_requires=[
        'numpy', 'pandas', 'requests', 'BeautifulSoup', 'sqlalchemy',
        'pymysql'
    ],

    # Entry points for command line integration
    entry_points="",

    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },
)