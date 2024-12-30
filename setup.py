from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

setup(
    name="pyspark-explorer",  # Required
    version="0.0.3",  # Required
    description="Explore data files with pyspark",  # Optional
    long_description=long_description,  # Optional
    long_description_content_type="text/markdown",  # Optional (see note above)
    url="https://github.com/krzys9876/pyspark_explorer",  # Optional
    author="Krzysztof Ruta",  # Optional
    author_email="krzys9876@gmail.com",  # Optional
    classifiers=[  # Optional
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
    ],
    keywords="pyspark, spark, explorer, data",  # Optional
    package_dir={"": "src"},  # Optional
    packages=find_packages(where="src"),  # Required
    python_requires=">=3.11, <4",
    install_requires=["pyspark>=3.5.1, <4.0.0", "textual>=1.0.0"],  # Optional
    project_urls={  # Optional
        "Source": "https://github.com/krzys9876/pyspark_explorer",
    },
)