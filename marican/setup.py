from setuptools import setup, Extension

setup(name='marican', version='1.0.0',
      description='Wow. So fast. Much indexed.',
      author='Ludvig Ericson',
      author_email='ludvig@lericson.se',
      url='http://lericson.se/',
      ext_modules=[Extension('marican', ['src/marican.c'])])