import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dagster-meltano",
    version="1.0.0",
    author="Jules Huisman",
    author_email="jules.huisman@quantile.nl",
    description="A dagster plugin that allows you to run integrate your Meltano project inside Dagster.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/quantile-development/dagster-meltano",
    project_urls={
        "Bug Tracker": "https://github.com/quantile-development/dagster-meltano/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "dagster~=1.0.7",
        "dagster-pandas~=0.16.7",
        "pandas~=1.4.4",
        "requests~=2.28.1",
        "attrs~=22.1.0",
        "agate~=1.6.3",
    ],
    extras_require={
        "meltano": [
            "meltano>2.4,<=2.5",
        ],
    },
    packages=["dagster_meltano", "dagster_dbt"],
    python_requires=">=3.6",
)
