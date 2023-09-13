from setuptools import setup

setup(
    name="deta",
    version="1.2.0",
    description="Python SDK for Deta Base & Deta Drive.",
    url="http://github.com/deta/deta-python",
    author="Deta",
    author_email="hello@deta.sh",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=["deta", "deta._async"],
    extras_require={
        "async": ["aiohttp>=3,<4"],
    },
)
