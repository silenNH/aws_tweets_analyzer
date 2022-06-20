#This script is used for local validation 
from lambda_function import lambda_handler

res=lambda_handler(None,None)

print(res)