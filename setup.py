import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name="grand-graph",
    version="0.2.1",
    author="Jordan Matelsky",
    author_email="opensource@matelsky.com",
    description="Graph database wrapper for non-graph datastores",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aplbrain/grand",
    packages=setuptools.find_packages(),
    install_requires=[
        "networkx>=2.4",
        "numpy",
        "pandas",
    ],
    extra_requires={
        "sql": ["SQLAlchemy>=1.3"],
        "dynamodb": ["boto3"],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
