#!/usr/bin/env python3

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="bbw",
    author="Renat Shigapov, Philipp Zumstein, Jan Kamlah, Lars Oberlaender, Joerg Mechnich, Irene Schumm",
    license="MIT",
    description="Library for semantic annotation of tabular data with the Wikidata knowledge graph",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/UB-Mannheim/bbw",
    use_scm_version={"local_scheme": "no-local-version"},
    setup_requires=['setuptools_scm'],
    install_requires=[
	"ftfy>=5.8",
	"tqdm>=4.48.0",
	"pandas>=1.0.5",
	"streamlit>=0.70.0",
	"requests>=2.23.0",
	"numpy>=1.18.4",
	"beautifulsoup4>=4.9.3",
    ],
    scripts=['bbw_cli.py','bbw_gui.py','bbw_parallel.sh'],
    packages=find_packages(),
    classifiers=[
	"License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
	"Intended Audience :: Education",
        "Intended Audience :: Science/Research",
	"Operating System :: OS Independent",
	"Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
	"Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.6',
)
