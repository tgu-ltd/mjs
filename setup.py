from setuptools import setup, find_packages

setup(
    zip_safe=False,
    name='mqtt2sql',
    version='0.0.1',
    url='https://github.com/tgu-ltd/mqtt2sql',
    author_email='dev@tgu-ltd.uk',
    author='tgu-ltd',
    license='GPL2.1',
    packages=find_packages(exclude=['tests']),
    install_requires=['paho-mqtt'],
    classifiers=[
        'Development Status :: 1 - Alpha',
        'License :: GPL 2.1',
        'Programming Language :: Python :: 3.6',
        'Topic :: Database :: Communications :: sqlite3 :: mqtt',
        "Operating System :: Linux",
    ],
    keywords='mqtt sqlite',
    entry_points={'console_scripts': ['m2s=mqtt2sql.cmd:start']},
    include_package_data=True,
    description='Store mqtt message into a sqlite database',
    long_description='Store mqtt message into a sqlite database. Auto table creation. Plugin listen and digest',
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest-ordering', 'pytest', 'pytest-ordering',
    ],
)
