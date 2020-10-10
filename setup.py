import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

["sqlalchemy"]

setuptools.setup(
    name="grand",
    version="0.1.0",
    author="Jordan Matelsky",
    author_email="opensource@matelsky.com",
    description="Graph database wrapper for non-graph datastores",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/j6k4m8/grand-python",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
