import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dagster-meltano",
    version="0.0.3",
    author="Jules Huisman",
    author_email="jules.huisman@quantile.nl",
    description="A Dagster plugin that allows you to run Meltano in Dagster",
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
    extras_require={
        "development": ["pytest==6.2", "meltano==1.85", "dagit==0.13", "black", "isort", "pylint"]
    },
    packages=["dagster_meltano"],
    python_requires=">=3.6",
)
