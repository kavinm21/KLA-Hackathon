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
def time_function(exec_time):
    time.sleep(exec_time)

# function to execute activity and help in concurrency
def execute_activity(task, str_template, txt_lines, lock=0):
    
    temp = str_template.copy()
    line = log_line(temp) + " Entry\n"
    txt_lines.append(line)
    task_attr = list(task.keys())
    # change to required conditional expression
    condition = 1
    if lock != 0:
        lock.acquire()
    print("Current Task: ", temp)
    if "Execution" in task_attr:
        sub_flow = task['Activities']
        act_on_activities(sub_flow, temp, txt_lines, lock)
    else:
        if task['Function'] == 'TimeFunction' and condition:
            
            op_str = log_line(temp)
            task_input = task['Inputs']
            exec_time = int(task_input['ExecutionTime'])
            op_str += " Executing TimeFunction("
            op_str += task_input['FunctionInput'] + ","
            op_str += str(exec_time) + ")\n"
            txt_lines.append(op_str)
            time_function(exec_time)
    if lock != 0:
        lock.release()
    line = log_line(temp) + " Exit\n"
    txt_lines.append(line)
        
#here the activities are parsed
def act_on_activities(activity, str_template, txt_lines, lock = 0):
    
    #parsing the activities to execute them
    list_tasks = list(activity.keys())
    threads = []
    temp = str_template.copy()
    for x in list_tasks:
        task = activity[x]
        temp.append(x)
        if lock != 0:
            t1 = td.Thread(target=execute_activity, args=(task, temp, txt_lines, lock))
            threads.append(t1)
        else:
            execute_activity(task, temp, txt_lines)
    if lock != 0:
        for x  in threads:
            x.start()
        for x in threads:
            x.join()
            
if __name__ == "__main__":
    
    with open("Files\Milestone1\Milestone1B.yaml") \
    as ip_file:
        ip_data = yaml.load(ip_file, Loader=yaml.FullLoader)
        log_file = open("Sample-OP-Files\Milestone1B.txt", "w+")
        workflow_keys = list(ip_data.keys())
        Lock = td.Lock()
        for x in workflow_keys:
            txt_line = []
            str_template = [x]
            task_string = log_line(str_template) + " Entry\n"
            txt_line.append(task_string)
            workflow = ip_data[x]
            exec_ord = workflow['Execution']
            activity = workflow['Activities']
            act_on_activities(activity, str_template, txt_line, Lock)
            task_string = log_line(str_template) + " Exit\n"
            txt_line.append(task_string)
            log_file.writelines(txt_line)
            print("Workflow {} has been logged".format(x))
        print("File Created!")
        log_file.close()
    print("log file closed")
    
            
            