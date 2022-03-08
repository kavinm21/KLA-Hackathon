# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 09:02:21 2022

@author: kavin
"""

import yaml
import pandas as pd

def act_on_activities(activity):
    list_tasks = activity.keys()
    print(list_tasks)
    
    
if __name__ == "__main__":
    
    with open("Files\Examples\Milestone1\Milestone1_Example.yaml") \
    as ip_file:
            ip_data = yaml.load(ip_file, Loader=yaml.FullLoader)
            workflow_keys = ip_data.keys()
            workflow = ip_data['M1SampleWorkFlow']
            type_exec = workflow['Type']
            exec_ord = workflow['Execution']
            activity = workflow['Activities']
            act_on_activities(activity)
            