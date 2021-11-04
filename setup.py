from setuptools import setup, find_packages

setup(
        name='rndflow',
        author='D Demidov, M Galimov',
        author_email='mail@rndflow.com',
        version='0.1.0',
        description='Support module for RnDflow jobs',
        include_package_data=True,
        packages=find_packages(),
        entry_points={
            'console_scripts': [
                'rndflow-execute=rndflow.execute:main'
                ]
            },
        )
