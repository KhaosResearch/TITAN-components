from os.path import abspath, dirname, join

from setuptools import find_packages, setup

basedir = abspath(dirname(__file__))

with open(join(basedir, "README.md"), encoding="utf-8") as f:
    README = f.read()

with open(join(basedir, "drama_titan", "__init__.py"), "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, VERSION = line.split(version_marker)
            VERSION = VERSION.strip().strip('"')
            break
    else:
        raise RuntimeError("Version not found on __init__")

install_requires = [
    # core
    #"drama>=4.1.0",
    # other
    "requests",
    "docker",
    "dask[dataframe]",
    "pandas>=1.0.4",
    "numpy>=1.18.5",
    "scipy>=1.4.1",
    "sklearn",
    "keras>=2.4.3",
    "tensorflow>=2.2.0",
    "dnspython>=2.0.0"
]

setup(
    name="drama_titan",
    version=VERSION,
    description="TITAN components for drama",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Antonio Benitez-Hidalgo",
    author_email="antoniobenitezhid@gmail.com",
    maintainer="Antonio Benitez-Hidalgo",
    maintainer_email="antoniobenitezhid@gmail.com",
    license="MIT",
    url="https://github.com/benhid/drama-titan",
    packages=find_packages(exclude=["test_"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=install_requires
)