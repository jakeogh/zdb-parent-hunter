# -*- coding: utf-8 -*-
"""
zfs-parent-index - Look for zfs id's with specific parent(s).
"""
from setuptools import find_packages, setup
import sys
if not sys.version_info[0] == 3:
    sys.exit("Sorry, Python >= 3.7 is required. Use: \'python3 setup.py install\'")

dependencies = []
version = 0.01

setup(
    name="zfs-parent-index",
    version=version,
    url="https://github.com/jakeogh/zfs-parent-index",
    license='PUBLIC DOMAIN',
    author="jakeogh",
    author_email="github.com@v6y.net",
    description='',
    long_description=__doc__,
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=False,
    platforms='any',
    install_requires=dependencies,
    entry_points={
        'console_scripts': [
            'zfs-parent-index = zfs_parent_index.zfs_parent_index:index_parents',
        ],
    },
    classifiers=[
        # 'Development Status :: 1 - Planning',
        # 'Development Status :: 2 - Pre-Alpha',
        # 'Development Status :: 3 - Alpha',
        'Development Status :: 4 - Beta',
        # 'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX',
        'Operating System :: MacOS',
        'Operating System :: Unix',
        'Operating System :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
