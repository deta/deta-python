"""
Packaging for the SDK
"""
import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

requirements = ["python-dotenv"]

setuptools.setup(
    name="deta",
    version="0.8",
    description="Python SDK for Deta Base & Deta Drive.",
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
