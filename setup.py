from setuptools import find_packages, setup

setup(
    name="pde_dagster",
    packages=find_packages(exclude=["pde_dagster_tests"]),
    install_requires=[
        "boto3",
        "dagster",
        "dagster-cloud",
        "geopy",
        "google-api-python-client",
        "matplotlib",
        "numpy",
        "oauth2client",
        "openpyxl",
        "pandas",
        "pandas_datareader",
        "psycopg2-binary",
        "scipy",
        "statsmodels",
        "textblob",
        "tweepy",
        "wordcloud",
        "yfinance",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
