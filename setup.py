from setuptools import setup, find_packages

setup(
    name = "mango_pulsar_client",
    version = "0.1.0",
    keywords = ("pulsar","client"),
    description = "pulsar client base on pulsar official client",
    long_description = "base on pulsar official rest api",
    license = "MIT Licence",

    url = "https://github.com/DavidCate/pulsar_test",
    author = "DavidLiu",
    author_email = "1049698300@qq.com",

    packages = find_packages(),
    include_package_data = True,
    platforms = "any",
    install_requires = ['ujson','requests','pulsar-client']
)
