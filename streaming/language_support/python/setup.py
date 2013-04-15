try:
    from setuptools import setup, Feature
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, Feature

f = open("README.rst")
try:
    try:
        readme_content = f.read()
    except:
        readme_content = ""
finally:
    f.close()


setup(
    name='pymongo_hadoop',
    version='1.1.0',
    maintainer="Michael O'Brien",
    maintainer_email='mikeo@10gen.com',
    long_description=readme_content,
    packages=['pymongo_hadoop'],
    url='https://github.com/mongodb/mongo-hadoop',
    keywords=["mongo", "mongodb", "hadoop", "hdfs", "streaming"],
    install_requires=[
        'pymongo'
    ],
)
