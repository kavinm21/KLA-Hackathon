# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 09:02:21 2022

@author: kavin
"""

import yaml
import pandas as pd
import datetime as dt


def time_function(curr_time, exec_time):
    curr_time = curr_time + dt.timedelta(seconds=exec_time)
    
def act_on_activities(activity, curr_time):
    lines = []
    list_tasks = list(activity.keys())
    for x in list_tasks:
        task = activity[x]
        task_attr = list(task.keys())
        if "Execution" in task_attr:
            sub_flow = task['Activities']
        else:
            if task['Function'] == 'TimeFunction':
                time_function(curr_time, exec_time)
                op_str = str()
                lines.append()
                
if __name__ == "__main__":
    
    with open("Files\Examples\Milestone1\Milestone1_Example.yaml") \
    as ip_file:
            ip_data = yaml.load(ip_file, Loader=yaml.FullLoader)
            workflow_keys = ip_data.keys()
            workflow = ip_data['M1SampleWorkFlow']
            type_exec = workflow['Type']
            exec_ord = workflow['Execution']
            activity = workflow['Activities']
            curr_time = dt.datetime.now()
            txt_lines = act_on_activities(activity, curr_time)
            log_file = open("Milestone1_Example.txt")
            log_file.writelines(txt_lines)
            
            
            