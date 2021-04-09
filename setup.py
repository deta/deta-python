from setuptools import setup

setup(
    name="deta",
    use_incremental=True,
    setup_requires=["incremental"],
    install_requires=["incremental"],
    description="Deta lib for Python 3",
    url="http://github.com/deta/deta-python",
    long_description="Deta SDK for python. Documentation available at https://docs.deta.sh/docs/base/sdk.",
    repository="https://test.pypi.org/legacy/",
    author="Deta",
    author_email="hello@deta.sh",
    license="MIT",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=["deta"],
)
