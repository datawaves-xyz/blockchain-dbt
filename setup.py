import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


long_description = read('README.md.md') if os.path.isfile("README.md.md") else ""

setup(
    name='blockchain-dbt',
    version='0.3.7',
    author='Datawaves',
    author_email='team@datawaves.xyz',
    description='The schema transformer and dbt utils for blockchain data.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/datawaves/blockchain-dbt',
    packages=find_packages(exclude=["test.*", "test"]),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    keywords='blockchain, dbt',
    python_requires='>=3.8,<4',
    install_requires=[
        'pyaml==21.10.1',
        'ruamel.yaml==0.17.21',
        'eth-utils==1.10.0',
        'click==7.1.2',
    ],
    extras_require={
        'dev': [
            "pytest~=4.3.0",
            "pyspark==3.2.1"
        ]
    },
    entry_points={
        'console_scripts': [
            'bdbt=bdbt.cli:cli'
        ]
    },
    project_urls={
        'Bug Reports': 'https://github.com/datawaves/blockchain-dbt/issues',
        'Source': 'https://github.com/datawaves/blockchain-dbt',
    },
    tests_require=[
        'nose==1.3.7',
        'unittest2>=1.0.0'
    ]
)
