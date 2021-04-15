"""
Packaging for the SDK
"""
import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

requirements = ["python-dotenv"]

setuptools.setup(
    name="deta",
    version="0.4",
    description="Deta lib for Python 3",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/deta/deta-python",
    author="Deta",
    author_email="hello@deta.sh",
    license="MIT",
    packages=setuptools.find_packages(exclude=["dist", "build", "*.egg-info", "tests"]),
    install_requires=requirements,
    extras_require={"dev": ["black", "setuptools", "wheel", "twine", "mypy"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
