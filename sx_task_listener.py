import os
import signal
import json
import subprocess
from time import sleep, time


print('SX Batcher: Task Listener Starting')
task_file_path = r'E:\sx_batcher_task_list.txt'


class ExitHandler:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True


class SXGlobals(object):
    def __init__(self):
        self.task_list = []
        self.exporting = False
        self.remote_exporting = False

    def __del__(self):
        print('Exiting sxglobals')


def load_json(file_path):
    try:
        with open(file_path, 'r') as input:
            temp_array = []
            temp_array = json.load(input)
            input.close()
        return temp_array
    except ValueError:
        print('SX Batcher Error: Invalid JSON file.')
        return []
    except IOError:
        print('SX Batcher Error: File not found!')
        return []


def check_tasks():
    sxglobals.task_list = load_json(task_file_path)
    if len(sxglobals.task_list) > 0:
        return True
    else:
        return False


def start_export():
    sxglobals.exporting = True
    batch_args = ['python3', 'sx_manager.py', '-r']
    for task in sxglobals.task_list:
        batch_args.append(task)
    subprocess.run(batch_args)

    f = open(task_file_path, 'w')
    f.write('[]')
    f.close()


sxglobals = SXGlobals()
exit_handler = ExitHandler()


while not exit_handler.kill_now:
    if not sxglobals.exporting:
        if check_tasks():
            start_export()
        else:
            sleep(0.01)

print('SX Batcher: Task Listener Stopped')
