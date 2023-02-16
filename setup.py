from setuptools import (
    setup,
    find_packages,
)


setup(
    name='aws_dynamodb_module',
    version='0.0.1',
    description='Amazon DynamoDB Module',
    url='git@github.com:VoltaApp/aws-dynamodb-module.git',
    author='Steve',
    author_email='vkhanhqui@gmail.com',
    license='unlicense',
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    zip_safe=False,
    install_requires=[
        "boto3",
        "pydantic[email]",
    ],
    python_requires='>=3.9',
    project_urls={
        'Documentation': 'https://github.com/VoltaApp/aws-dynamodb-module',
        'Bug Reports': 'https://github.com/VoltaApp/aws-dynamodb-module/issues',
        'Source Code': 'https://github.com/VoltaApp/aws-dynamodb-module',
    },
)
