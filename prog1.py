# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 09:02:21 2022

@author: kavin
"""

import yaml
import pandas as pd
import threading as td
import datetime as dt
import time

#function to create a log line
def log_line(str_template):
    string = str(dt.datetime.now()) + ';'
    for x in range(len(str_template)):
        string += str_template[x]
        if x < (len(str_template) - 1):
            string += '.'
    return string

#function for data_load
def data_load(fname):
    df = pd.read_csv(fname)
    
#function for time_function
def time_function(exec_time, lock = 0):
    #pause execution
    if lock == 0:
        time.sleep(exec_time)
    else:
        lock.acquire()
        time.sleep(exec_time)
        lock.release()

#here the activities are parsed
def act_on_activities(activity, str_template, txt_lines, lock = 0):
    
    #parsing the activities to execute them
    list_tasks = list(activity.keys())
    threads = []
    for x in list_tasks:
        task = activity[x]
        str_template.append(x)
        line = log_line(str_template) + " Entry\n"
        txt_lines.append(line)
        task_attr = list(task.keys())
        # change to required conditional expression
        condition = 1
        if "Execution" in task_attr:
            sub_flow = task['Activities']
            act_on_activities(sub_flow, str_template, txt_lines)
        else:
            if task['Function'] == 'TimeFunction' and condition:
                op_str = log_line(str_template)
                task_input = task['Inputs']
                exec_time = int(task_input['ExecutionTime'])
                op_str += " Executing TimeFunction("
                op_str += task_input['FunctionInput'] + ","
                op_str += str(exec_time) + ")\n"
                txt_lines.append(op_str)
                if lock != 0:
                    threads.append(td.Thread(target=time_function, args(exec_time,lock)))
                else:
                time_function(exec_time)
        line = log_line(str_template) + " Exit\n"
        txt_lines.append(line)
        str_template.pop(-1)
    
if __name__ == "__main__":
    
    with open("Files\Milestone1\Milestone1B.yaml") \
    as ip_file:
            ip_data = yaml.load(ip_file, Loader=yaml.FullLoader)
            log_file = open("Sample-OP-Files\Milestone1B.txt", "a+")
            workflow_keys = list(ip_data.keys())
            Lock = td.Lock()
            for x in workflow_keys:
                txt_lines = []
                str_template = [x]
                task_string = log_line(str_template) + " Entry\n"
                txt_lines.append(task_string)
                workflow = ip_data[x]
                exec_ord = workflow['Execution']
                activity = workflow['Activities']
                lock = 0
                act_on_activities(activity, str_template, txt_lines, lock)
                task_string = log_line(str_template) + " Exit\n"
                txt_lines.append(task_string)
                log_file.writelines(txt_lines)
                print("Workflow {} has been logged".format(x))
            print("File Created!")
            log_file.close()
            
            
            