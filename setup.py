import setuptools
from setuptools import find_packages
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name="cvm_sqlite",
    version="1.0.0",
    author="Eduardo Ramon Resser",
    author_email="eduresser@gmail.com",
    description="A Python tool for downloading, processing, and storing data from the Brazilian Securities and Exchange Commission (CVM - Comissão de Valores Mobiliários) in a SQLite database.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/eduresser/cvm-sqlite",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    license='MIT',
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.12",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.12',
    install_requires=[
        'beautifulsoup4>=4.12.3',
        'pandas>=2.2.3',
        'requests>=2.32.3',
        'tqdm>=4.67.0'
    ]
)
