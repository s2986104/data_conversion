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
)
