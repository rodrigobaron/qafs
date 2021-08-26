from setuptools import setup, find_packages


setup(
    name="feature_store",
    packages=find_packages(),
    version="0.0.1",
    description="Feature Store",
    author="Rodrigo Baron",
    url="",
    extras_require={
        "dev": ["pytest"],
    },
)