from setuptools import find_packages,setup
from typing import List

E_DOT='-e .'
def get_requirements(file_path=str)->List[str]:
    '''
    This function will give list of requirements

    '''
    requirements=[]
    with open(file_path) as file_obj:
        requirements=file_obj.readlines()
        [req.replace("\n","") for req in requirements]

        if E_DOT in requirements:
            requirements.remove(E_DOT)
    
    return requirements

setup(
name='brooksDatapipe',
version='0.0.1',
author='Varjit',
author_email='varjit.gupta-3rdpty@brooksautomation.com',
packages=find_packages(),
install_requires=get_requirements('requirements.txt')
)
