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

file_dict = {}

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
    return [df, len(df.index)]

#function for time_function
def time_function(exec_time):
    time.sleep(exec_time)

# function to execute activity and help in concurrency
def execute_activity(task, str_template, txt_lines, lock=0):
    
    temp = str_template.copy()
    line = log_line(temp) + " Entry\n"
    print(line)
    txt_lines.append(line)
    task_attr = list(task.keys())
    # change to required conditional expression
    condition = 1
    if lock != 0:
        lock.acquire()
    if "Execution" in task_attr:
        sub_flow = task['Activities']
        if task['Execution'] == "Concurrent":
            lock = td.Lock()
        else:
            lock = 0
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
        elif task['Function'] == 'DataLoad' and condition:
            op_str = log_line(temp)
            task_input = task['Inputs']
            fname = task_input['Filename']
            op_str += "Executing DataLoad(" + fname + ')\n'
            txt_lines.append(op_str)
            file_data = data_load(txt_lines)
            file_dict[temp[-1]] = file_data
    if lock != 0:
        lock.release()
    line = log_line(temp) + " Exit\n"
    print(line)
    txt_lines.append(line)
        
#here the activities are parsed
def act_on_activities(activity, str_template, txt_lines, lock = 0):
    
    #parsing the activities to execute them
    list_tasks = list(activity.keys())
    threads = []
    temp_act = str_template.copy()
    for x in list_tasks:
        task = activity[x]
        temp_act.append(x)
        print("Current Task: ", temp_act)
        if lock != 0:
            t1 = td.Thread(target=execute_activity, args=(task, temp_act, txt_lines, lock))
            threads.append(t1)
            temp_act.pop(-1)
        else:
            execute_activity(task, temp_act, txt_lines)
            temp_act.pop(-1)
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
            if exec_ord == "Sequential":
                Lock = 0
            act_on_activities(activity, str_template, txt_line, Lock)
            task_string = log_line(str_template) + " Exit\n"
            txt_line.append(task_string)
            log_file.writelines(txt_line)
            print("Workflow {} has been logged".format(x))
        print("File Created!")
        log_file.close()
    print("log file closed")
                
            