from setuptools import setup

__version__ = "v1.0.22"

setup(
    name="sentinel_downloader",
    version=__version__,
    description="Utilities for downloading Landsat and Sentinel products from USGS",
    url="https://github.com/sscullen/sentinel_downloader.git",
    author="Shaun Cullen",
    author_email="shaun@cullen.io",
    license="MIT",
    packages=["sentinel_downloader"],
    zip_safe=False,
)
