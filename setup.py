from setuptools import setup, Extension

setup(name='England Weather Forecast',
      version='1.0',
      description='Weather Visualization and Modelling Tool',
      author='hcl4517',
      packages=['level_forecast_tools'],
      include_package_data=True,
      package_data={"level_forecast_tools": ['data/*.csv']},
      data_files=[("level_forecast_tools/data/archive/", [])]
      )
