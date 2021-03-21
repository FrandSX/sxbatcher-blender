import argparse
import subprocess
import codecs
import multiprocessing
import time
import json
import sys
from multiprocessing import Pool
import os


def get_args():
    conf = load_conf()
    catalogue_path = conf.get('catalogue_path')
    export_path = conf.get('export_path')

    if catalogue_path is not None:
        catalogue_path = catalogue_path.replace('//', os.path.sep) if os.path.isfile(catalogue_path.replace('//', os.path.sep)) else None
    if export_path is not None:
        export_path = export_path.replace('//', os.path.sep) if os.path.isdir(export_path.replace('//', os.path.sep)) else None

    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--open', default=catalogue_path, help='Open a Catalogue file')
    parser.add_argument('-a', '--all', action='store_true', help='Export the entire Catalogue')
    parser.add_argument('-c', '--category', help='Export all objects in a category (Default, Paletted...')
    parser.add_argument('-f', '--filename', help='Export an object by filename')
    parser.add_argument('-t', '--tag', help='Export all tagged objects')
    parser.add_argument('-e', '--exportpath', default=export_path, help='Local folder where remote exports are collected to')
    parser.add_argument('-l', '--listonly', action='store_true', help='Do not export, only list objects that match the other arguments')
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
        print('SX Node Manager Error: Invalid JSON file.')
        return {}
    except IOError:
        print('SX Node Manager Error: File not found!')
        return {}


def load_conf():
    if os.path.isfile(os.path.realpath(__file__).replace('sx_node_manager.py', 'sx_conf.json')):
        conf_path = os.path.realpath(__file__).replace('sx_node_manager.py', 'sx_conf.json')
        return load_json(conf_path)
    else:
        return {}


def load_asset_data(catalogue_path):
    if os.path.isfile(catalogue_path):
        return load_json(catalogue_path)
    else:
        print('SX Node Manager: Invalid Catalogue path')
        return {}


def load_nodes():
    if os.path.isfile(os.path.realpath(__file__).replace('sx_node_manager.py', 'sx_conf.json')):
        conf_path = os.path.realpath(__file__).replace('sx_node_manager.py', 'sx_conf.json')
        conf = load_json(conf_path)
        nodes_raw = conf['nodes']

        # Check node readiness
        nodes = []
        for node in nodes_raw:
            if node['os'] == 'win':
                if (subprocess.call(['ssh', node['user']+'@'+node['ip'], 'if exist sxbatcher-blender/sx_batch_node.py echo %errorlevel%'])) == 0:
                    nodes.append(node)
            else:
                if subprocess.call(['ssh', node['user']+'@'+node['ip'], 'test -e sxbatcher-blender/sx_batch_node.py']) == 0:
                    nodes.append(node)

        if len(nodes) == 0:
            print('SX Node Manager: No network nodes')
        else:
            print('SX Node Manager: Active Nodes')
            for node in nodes:
                print('Node:', node['ip'], ' / Cores:', node['numcores'], ' / OS:', node['os'])

        return nodes
    else:
        return []


def get_source_files():
    asset_path = None
    export_path = None

    args = get_args()
    category = str(args.category)
    filename = str(args.filename)
    tag = str(args.tag)
    catalogue_path = str(args.open)
    if args.open is not None:
        asset_dict = load_asset_data(catalogue_path)

    source_files = []
    if args.open is None:
        print('SX Node Manager Error: No Catalogue specified')
    else:
        if len(asset_dict) > 0:
            if args.all:
                for category in asset_dict.keys():
                    for key in asset_dict[category].keys():
                        source_files.append(key)
            else:
                if args.category is not None:
                    if category in asset_dict.keys():
                        for key in asset_dict[category].keys():
                            source_files.append(key)
                if args.filename is not None:
                    for category in asset_dict.keys():
                        for key in asset_dict[category].keys():
                            if filename in key:
                                source_files.append(key)
                if args.tag is not None:
                    for category in asset_dict.keys():
                        for key, values in asset_dict[category].items():
                            for value in values:
                                if tag == value:
                                    source_files.append(key)
                if (args.category is None) and (args.filename is None) and (args.tag is None):
                    print('SX Node Manager: Nothing selected for export')
        else:
            print('SX Node Manager Error: Invalid Catalogue')

    if len(source_files) > 0:
        source_files = list(set(source_files))

    return source_files


def sx_batch(task):
    p0 = subprocess.Popen(['ssh', task[0]+'@'+task[1], task[2]])
    sts = p0.wait()

    p1 = subprocess.Popen(['ssh', task[0]+'@'+task[1], task[3]])
    sts = p1.wait()

    # ssh = subprocess.Popen('ssh '+task[0]+'@'+task[1]+' '+cmd0, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # result = ssh.stdout.readlines()
    # for line in result:
    #    print(line.decode('utf-8').strip('\n'))

    # ssh = subprocess.Popen('ssh '+task[0]+'@'+task[1]+' '+cmd0, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # result = ssh.stdout.readlines()
    # for line in result:
    #    print(line.decode('utf-8').strip('\n'))


def sx_collect(collect_task):
    p = subprocess.Popen(['scp', '-r', collect_task[0]+'@'+collect_task[1]+':'+'sx_batch_temp/*', collect_task[2]])
    sts = p.wait()


# ------------------------------------------------------------------------
#    NOTE: The catalogue file should be located in the root
#          of your asset folder structure.
# ------------------------------------------------------------------------
if __name__ == '__main__':
    args = get_args()

    if args.exportpath is not None:
        export_path = os.path.abspath(args.exportpath)
    else:
        print('SX Node Manager: Export collection path not specified!')

    nodes = load_nodes()
    if len(nodes) > 0:
        source_files = get_source_files()

        # -----------------------------------------------------------------
        #    NOTE: SX Node Manager expects sxbatcher-blender folder to be
        #          located in user home folder. Adapt as necessary!
        # -----------------------------------------------------------------
        job_length = len(source_files)
        tasks = []
        i = 0
        while i < job_length:
            for j, node in enumerate(nodes):
                user = node['user']
                ip = node['ip']
                os = node['os']
                numcores = int(node['numcores'])
                nodefiles = source_files[i:(i + numcores)]
                if len(nodefiles) > 0:
                    if os == 'win':
                        cmd0 = 'mkdir sx_batch_temp'
                    else:
                        cmd0 = 'mkdir -p ~/sx_batch_temp'
                    cmd1 = 'python3 ~/sxbatcher-blender/sx_batch_node.py -r'
                    for file in nodefiles:
                        cmd1 += ' '+file
                    cmd1 += ' -e ~/sx_batch_temp/'
                    # cmd += '&&'+'rm -rf ~/sx_batch_temp'

                    tasks.append((user, ip, cmd0, cmd1))
                i += numcores

        collect_tasks = []
        for node in nodes:
            collect_tasks.append((node['user'], node['ip'], export_path))

        if not args.listonly and (len(source_files) > 0):
            then = time.time()
            print('\n'+'SX Node Manager: Tasking nodes')

            with Pool(processes=len(nodes)) as pool:
                pool.map(sx_batch, tasks)

            with Pool(processes=len(nodes)) as coll_pool:
                coll_pool.map(sx_collect, collect_tasks)

            now = time.time()
            print('SX Node Manager: Export Finished!')
            print('Duration:', now-then, 'seconds')
            print('Objects exported:', len(source_files))
