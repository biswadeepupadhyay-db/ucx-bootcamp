"""Wrapper for ./deploy-ws & deploy_ws_resources.py"""

import os
import sys
import json
import random
import string
import datetime
import subprocess
from typing import Optional
from utils import *





input_var = input("Try your luck with Databricks... [default=yes]: ") or "yes"



# Example usage
variables = {
    'age': 25,
    'username': "Biswadeep Upadhyay",
    'enable_feature': True,
    'access_count': 100
}

def deploy_workspace():
    pass

def deploy_resources():
    pass

def start():
    print("""Welcome to Databricks bootcamp for UCX...""")
    if input_var == "yes":
        deploy_workspace()
    deploy_resources()

start()