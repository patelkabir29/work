# -*- coding: utf-8 -*-
"""
Created on Tue Jun 22 15:56:34 2021

@author: kpatel
"""

import boto3

_aws_access_key = 'AKIAZJJJ2C25Q5SOA4VC'
_aws_secret_access_key = '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'

session = boto3.Session(
    aws_access_key_id=_aws_access_key,
    aws_secret_access_key=_aws_secret_access_key
)

s3 = session.resource('s3')

# Print out bucket names
for bucket in s3.buckets.all():
    print(bucket.name)
    
client = boto3.client(
    'textract',
    aws_access_key_id=_aws_access_key,
    aws_secret_access_key=_aws_secret_access_key,
    region_name='us-east-1'
)

response = client.analyze_document(
    Document={
        'S3Object': {
            'Bucket': 'ipg-market-reports',
            'Name': 'jll/construction-pipeline/jll-construction-pipeline.pdf'        
        }
    },
    FeatureTypes=[
        'TABLES'
    ]
)