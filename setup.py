from setuptools import find_namespace_packages, setup, find_packages
import re


VERSION = open('VERSION').read()
LONG_DESCRIPTION = open('README.md').read()


with open('requirements.txt') as f:
    required = f.read().splitlines()


with open('requirements_dev.txt') as f:
    extras_required_dev = f.read().splitlines()


setup(
    name="qafs",
    version=VERSION,
    description="Quality Aware Feature Store.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Rodrigo Baron",
    author_email="baron.rodrigo0@gmail.com",
    url="https://github.com/rodrigobaron/qafs",
    license="GPL-3.0",
    package_dir={'': 'src'},
    packages=find_packages(where="src"),
    zip_safe=False,
    extras_require={
        "dev": extras_required_dev,
    },
    data_files=[('', ['VERSION', 'README.md', 'LICENSE'])],
    include_package_data=True,
    python_requires='>=3.7',
    install_requires=required,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
