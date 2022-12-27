from setuptools import find_packages, setup

setup(
    name="pde_dagster",
    packages=find_packages(exclude=["pde_dagster_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "boto3",
        "pandas",
        "matplotlib",
        "textblob",
        "tweepy",
        "wordcloud",
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
