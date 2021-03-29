import argparse
import subprocess
import codecs
import multiprocessing
import time
import json
import socket
import shutil
from multiprocessing import Pool, Value, Array
import os


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('10.255.255.255', 1))
        ip = s.getsockname()[0]
    except Exception:
        ip = '127.0.0.1'
    finally:
        s.close()
    return ip


ip_addr = get_ip()
nodename = 'SX Batch Node '+ip_addr


def get_args():
    conf = load_conf()
    blender_path = conf.get('blender_path')
    catalogue_path = conf.get('catalogue_path')
    export_path = conf.get('export_path')
    sxtools_path = conf.get('sxtools_path')

    if blender_path is not None:
        blender_path = blender_path.replace('//', os.path.sep) if os.path.isfile(blender_path.replace('//', os.path.sep)) else None
    if catalogue_path is not None:
        catalogue_path = catalogue_path.replace('//', os.path.sep) if os.path.isfile(catalogue_path.replace('//', os.path.sep)) else None
    if export_path is not None:
        export_path = export_path.replace('//', os.path.sep) if os.path.isdir(export_path.replace('//', os.path.sep)) else None
    if sxtools_path is not None:
        sxtools_path = sxtools_path.replace('//', os.path.sep) if os.path.isdir(sxtools_path.replace('//', os.path.sep)) else None

    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--blenderpath', default=blender_path, help='Blender executable location')
    parser.add_argument('-o', '--open', default=catalogue_path, help='Open a Catalogue file')
    parser.add_argument('-s', '--sxtools', default=sxtools_path, help='SX Tools folder')
    parser.add_argument('-e', '--exportpath', default=export_path, help='Export path')
    all_arguments, ignored = parser.parse_known_args()
    return all_arguments


def load_json(file_path):
    try:
        with open(file_path, 'r') as input:
            temp_dict = {}
            temp_dict = json.load(input)
            input.close()
        return temp_dict
    except ValueError:
        print(nodename + ' Error: Invalid JSON file.')
        return {}
    except IOError:
        print(nodename + ' Error: File not found!')
        return {}


def save_json(data):
    file_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_costs.json')
    with open(file_path, 'w') as output:
        temp_dict = {}
        temp_dict = data
        json.dump(temp_dict, output, indent=4)

        output.close()


def load_conf():
    if os.path.isfile(os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')):
        conf_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')
        return load_json(conf_path)
    else:
        return {}


def load_asset_data(catalogue_path):
    if os.path.isfile(catalogue_path):
        return load_json(catalogue_path)
    else:
        print(nodename + ': Invalid Catalogue path')
        return {}


def sx_init(result_array, task_array):
    global results, tasks
    results = result_array
    tasks = task_array


def sx_process(i):
    blender_path = tasks[i][0]
    source_file = tasks[i][1]
    script_path = tasks[i][2]
    export_path = tasks[i][3]
    sxtools_path = tasks[i][4]

    batch_args = [blender_path, "-b", "--factory-startup", "-noaudio", source_file, "-P", script_path, "--"]
    print('ba:', batch_args)

    if export_path is not None:
        batch_args.extend(["-x", export_path])

    if sxtools_path is not None:
        batch_args.extend(["-l", sxtools_path])

    # Primary method: spawns quiet workers
    then = time.time()
    with codecs.open(os.devnull, 'wb', encoding='utf8') as devnull:
        try:
            subprocess.check_call(batch_args, stdout=devnull, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            print('SX Batch Error:', source_file)

    # Comment above and uncomment below for for debugging (also add "-d" to batch args)
    # subprocess.run(batch_args)
    now = time.time()
    results[i] = round(now-then, 2)

# ------------------------------------------------------------------------
#    NOTE: The catalogue file should be located in the root
#          of your asset folder structure.
# ------------------------------------------------------------------------
if __name__ == '__main__':
    asset_path = None
    export_path = None
    sxtools_path = None

    args = get_args()

    script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
    blender_path = str(args.blenderpath)
    catalogue_path = str(args.open)
    if args.open is not None:
        asset_path = os.path.split(catalogue_path)[0].replace('//', os.path.sep)
        asset_dict = load_asset_data(catalogue_path)
    if args.exportpath is not None:
        export_path = os.path.abspath(args.exportpath)
    else:
        print(nodename + ': Export path not specified, using paths defined in source files')
    if args.sxtools is not None:
        sxtools_path = os.path.abspath(args.sxtools)
    else:
        print(nodename + ' Warning: SX Tools path not specified')

    source_files = []
    if args.blenderpath is None:
        print(nodename + ' Error: Blender path not specified')
    elif args.open is None:
        print(nodename + ' Error: No Catalogue or folder specified')
    else:
        if len(asset_dict) > 0:
            for category in asset_dict.keys():
                for key in asset_dict[category].keys():
                    file_path = key.replace('//', os.path.sep)
                    source_files.append(os.path.join(asset_path, file_path))
        else:
            print(nodename + ' Error: Invalid Catalogue')


        # Generate task definition for each headless Blender
        results = multiprocessing.Array('d', [0.0]*len(source_files))
        tasks = []
        for file in source_files:
            tasks.append((blender_path, file, script_path, export_path, sxtools_path))

        if len(source_files) > 0:
            N = len(tasks)
            print(nodename + ': Spawning workers ( max', multiprocessing.cpu_count(), ')')

            then = time.time()

            with Pool(processes=4, initializer=sx_init, initargs=(results, tasks), maxtasksperchild=1) as pool:
                pool.map(sx_process, range(N))

            now = time.time()

            costs = {}
            times = results[:]
            for i, file in enumerate(source_files):
                costs[os.path.basename(file)] = times[i]
                print(file, times[i])

            print(nodename + ':', len(source_files), 'full Catalogue export in', round(now-then, 2), 'seconds\n')
            save_json(costs)
