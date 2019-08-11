from setuptools import setup, find_packages

setup(
    zip_safe=False,
    name='mjs',
    version='0.0.1',
    url='https://github.com/tgu-ltd/mjs',
    author_email='dev@tgu-ltd.uk',
    author='tgu-ltd',
    license='GPL2.1',
    packages=find_packages(exclude=['tests']),
    install_requires=['paho-mqtt'],
    classifiers=[
        'License :: OSI Approved',
        'Topic :: Home Automation',
        'Programming Language :: Python :: 3',
    ],
    keywords='mqtt sqlite',
    entry_points={'console_scripts': ['mjs=mjs.cli:start']},
    include_package_data=True,
    description='Store mqtt message into a sqlite database',
    long_description='Store mqtt json messages into a sqlite database.',
    setup_requires=['pytest-runner'],
    tests_require=[
        'pytest-ordering', 'pytest', 'pytest-ordering',
    ],
)
