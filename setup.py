from distutils.core import setup
setup(
    name='acli',
    version='0.1',
    description='IIASA Accelerator python client',
    author='Wrufesh S',
    author_email='wrufesh@gmail.com',
    packages=['acli'],
    install_requires=['urllib3>=2.0.4'],
    python_requires='>=3.6',
)