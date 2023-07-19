from setuptools import setup, find_packages

with open('requirements.txt', 'r') as req_file:
    setup(
        name='ET_Products',
        version='0.0.1',
        install_requires=req_file.readlines(),
        packages=find_packages(
            include=['pipelines', 'pipelines.*', 'variables_dev', 'variables_dev.*']
        ),
        entry_points={'console_scripts': ['et_products = cdh_etl:et_products']},
        url='https://github.com/Etienne668/ET_Products',
        license='',
        author='oca',
        author_email='',
        py_modules=["et_products"],
        description='Simple ETL',
        zip_safe=False,
    )
    
