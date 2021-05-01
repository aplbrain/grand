import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

["sqlalchemy"]

setuptools.setup(
    name="grand-graph",
    version="0.2.0",
    author="Jordan Matelsky",
    author_email="opensource@matelsky.com",
    description="Graph database wrapper for non-graph datastores",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aplbrain/grand",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3",
        "networkx==2.4",
        "numpy==1.19.1",
        "pandas==1.1.0",
        "SQLAlchemy==1.3.18",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
