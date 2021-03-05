from setuptools import setup, find_packages

setup(
    name="algorunner",
    version=0.1,
    description="A simple script run the Image Matching algorithm and generate tsv output",
    packages=find_packages(),
    install_requires=[
        'beautifulsoup4>=4.9.3',
        'pandas>=1.2.1',
        'papermill>=2.3.2'],
    dependency_links=[
        'wmfdata @ git+ssh://git@github.com/wikimedia/wmfdata-python.git=wmfdata'
    ]
)
