# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 09:02:21 2022

@author: kavin
"""

import yaml
import pandas as pd
import datetime as dt
import time

#function to create a log line
def log_line(str_template):
    string = str(dt.datetime.now())
    for x in str_template:
        string += x
    return string
#function for time_function
def time_function(exec_time):
    #pause execution
    time.sleep(exec_time)

#here the activities are parsed
def act_on_activities(activity, txt_lines):
    
    #parsing the activities to execute them
    list_tasks = list(activity.keys())
    act_template = str_template + ""
    for x in list_tasks:
        task = activity[x]
        str_template.append(x)
        line = str(dt.datetime.now()) + str_template[0]
        line += str_template[1] + "Entry"
        txt_line.append()
        task_attr = list(task.keys())
        if "Execution" in task_attr:
            sub_flow = task['Activities']
        else:
            if task['Function'] == 'TimeFunction':
                time_function(exec_time)
                op_str = str(curr_time) + ';' + 
                lines.append()
                
if __name__ == "__main__":
    
    with open("Files\Examples\Milestone1\Milestone1_Example.yaml") \
    as ip_file:
            ip_data = yaml.load(ip_file, Loader=yaml.FullLoader)
            log_file = open("Milestone1_Example.txt", "a+")
            workflow_keys = list(ip_data.keys())
            for x in workflow_keys:
                txt_lines = []
                str_template = [x]
                task_string = log_line(str_template) + " Entry"
                txt_lines.append(task_string)
                workflow = ip_data['M1SampleWorkFlow']
                #type_exec = workflow['Type']
                exec_ord = workflow['Execution']
                activity = workflow['Activities']
                act_on_activities(activity, str_template, txt_lines)
                task_string = log_line(str_template) + "Exit"
                log_file.writelines(txt_lines)
                
            
            