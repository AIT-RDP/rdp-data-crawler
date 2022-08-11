import versioneer
from setuptools import find_packages, setup

setup(
    name="E3 Data Crawler",
    packages=find_packages(exclude=["test", "test.*"]),
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Pull-based translator to feed external data into Redis",
    author="AIT Austrian Institute of Technology",
    license="Proprietary",
    url="https://gitlab-intern.ait.ac.at/ees/rdp/e3-at-school/e3-data-crawler",
    setup_requires=["pytest-runner"],
    install_requires=[
        "pyyaml>=6.0",
        "redis>=4.3",
        "python-dotenv>=0.20",
        "requests>=2.28",
        "cachecontrol>=0.12"
    ],
    test_requires=["pytest"],
)