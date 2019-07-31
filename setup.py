from setuptools import setup, find_packages

setup(
    name='data_conversion',
    version='0.1',
    description='datamanager',
    # long_description=README + '\n\n' + CHANGES,
    classifiers=[
        'Programming Language :: Python',
        'Framework :: Pyramid',
    ],
    author='',
    author_email='',
    url='',
    keywords='',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'gen_collections=data_conversion.gen_collections:main',
            'gen_variables=data_conversion.gen_variables:main',
            'update_metadata=data_conversion.update_metadata:main'
        ]
    }
)
