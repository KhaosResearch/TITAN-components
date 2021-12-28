from os.path import abspath, dirname, join

from setuptools import find_packages, setup

basedir = abspath(dirname(__file__))

with open(join(basedir, "README.md"), encoding="utf-8") as f:
    README = f.read()

with open(join(basedir, "drama_enbic2lab", "__init__.py"), "r") as f:
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
    "drama",
    # other
    "pandas==1.2.1",
    "requests",
    "geopandas",
    "rarfile",
    "docker",
    "xlrd==2.0.1",
    "openpyxl==3.0.6",
    "jinja2",
    "matplotlib==3.3.4",
    "scikit-learn==0.24.1",
    "scipy==1.6.0",
    "numpy==1.20.0rc2",
    "pyreadstat==1.0.8",
    "pyhomogeneity==1.1",
]

package_data = {"templates": ["*.jinja"]}

setup(
    name="drama_enbic2lab",
    version=VERSION,
    description="Enbic2lab components to drama",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Khaos Research",
    author_email="jfaldana@uma.es",
    maintainer="Manuel Paneque Romero",
    maintainer_email="mpaneque@uma.es",
    license="MIT",
    url="https://github.com/KhaosResearch/drama-lifewatch",
    packages=find_packages(exclude=["test_"]),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=install_requires,
    include_package_data=True,
)
