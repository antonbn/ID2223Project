import setuptools

setuptools.setup(
    name="price-predict",
    install_requires=[
        "numpy",
        "pandas",
        "hopsworks",
        "entsoe-py",
        "forex-python",
    ],
    packages=setuptools.find_packages(),
    zip_safe=False,
)
