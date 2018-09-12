"""
This must run before setup.py. The compiled jar will be placed in the 'lib' folder, which should be
packaged together for pypi.
"""

import os
import re
import subprocess


def read_version(pom):
    """ Reads the version from pom.xml file.

    Assumes that the version tag containing the verdictdb version appears first in the pom file.
    """
    with open(pom) as f:
        context = f.read()
        version = re.search('<version>(.*?)</version>', context).group(1)
        return version

def build_and_copy(root_dir, lib_dir):
    if not os.path.exists(lib_dir):
        os.makedirs(lib_dir)

    pom_path = os.path.join(root_dir, '..', 'pom.xml')
    version = read_version(pom_path)
    jar_name = 'verdictdb-core-' + version + '-jar-with-dependencies.jar'
    os.chdir(root_dir)
    os.chdir('..')
    subprocess.check_call(
        ['mvn', '-DskipTests', '-DtestPhase=false', '-DpackagePhase=true', 'clean', 'package'])
    subprocess.check_call(['rm', '-rf', os.path.join(lib_dir, '*verdictdb*.jar')])
    subprocess.check_call(['cp', os.path.join('target', jar_name), lib_dir])
    os.chdir(root_dir)

if __name__ == "__main__":
    build_and_copy()
