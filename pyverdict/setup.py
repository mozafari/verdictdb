import setuptools
import os
import re
import subprocess
from build_lib import build_and_copy


def get_verdict_jar(lib_dir):
    lib_files = os.listdir(lib_dir)
    print(lib_files)

    def get_version(libfile):
        reobj = re.search('verdictdb-core-(.*?)-jar-with-dependencies.jar', libfile)
        if reobj is None:
            return (None, None)
        else:
            version = reobj.group(1)
            return (libfile, version)

    for libfile in lib_files:
        (libfile, version) = get_version(libfile)
        if version is not None:
            return (libfile, version)
    return (None, None)


root_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(root_dir, 'pyverdict', 'lib')

# if the directory does not exist, we create the directory, build a jar file, and copy the jar file
# to the lib directory.
if not os.path.exists(lib_dir):
    build_and_copy(root_dir, lib_dir)

(verdict_jar_file, version) = get_verdict_jar(lib_dir)
if version is None:
    build_and_copy(root_dir, lib_dir)
    (verdict_jar_file, version) = get_verdict_jar(lib_dir)

# print('PyVerdict version: ' + version)

setuptools.setup(
    name='pyverdict',
    version=version,
    description='Python interface for VerdictDB',
    url='http://verdictdb.org',
    author='Barzan Mozafari, Yongjoo Park',
    author_email='mozafari@umich.edu, pyongjoo@umich.edu',
    license='Apache License, Version 2.0',
    packages=setuptools.find_packages(),
    package_data={'pyverdict': ['lib/*.jar']},
    include_package_data=True,
    install_requires=[
        'py4j >= 0.10.7',
        'numpy >= 1.9'
    ]
 )
