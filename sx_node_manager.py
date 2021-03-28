import argparse
import subprocess
import codecs
import multiprocessing
import time
import json
import sys
from multiprocessing import Pool
import os


class sx_globals(object):
    def __init__(self):
        self.conf = None
        self.source_files = None
        self.nodes = None
        self.updaterepo = None
        self.staticvertexcolors = None
        self.subdivision = None
        self.palette = None
        self.exportpath = None
        self.listonly = None
        self.catalogue_path = None
        self.export_path = None
        self.category = None
        self.filename = None
        self.tag = None
        self.all = None


def init_globals():
    sxglobals.conf = load_json(os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json'))
    cp = sxglobals.conf.get('catalogue_path')
    ep = sxglobals.conf.get('export_path')
    if cp is not None:
        sxglobals.catalogue_path = cp.replace('//', os.path.sep) if os.path.isfile(cp.replace('//', os.path.sep)) else None
    if ep is not None:
        sxglobals.export_path = ep.replace('//', os.path.sep) if os.path.isdir(ep.replace('//', os.path.sep)) else None

    args = get_args()
    sxglobals.updaterepo = args.updaterepo
    sxglobals.all = args.all
    sxglobals.staticvertexcolors = args.staticvertexcolors
    sxglobals.listonly = args.listonly
    if args.subdivision is not None:
        sxglobals.subdivision = str(args.subdivision)
    if args.palette is not None:
        sxglobals.palette = str(args.palette)
    if args.exportpath is not None:
        sxglobals.export_path = os.path.abspath(args.exportpath)
    else:
        print('SX Node Manager: Export collection path not specified')

    if args.category is not None:
        sxglobals.category = str(args.category)
    if args.filename is not None:
        sxglobals.filename = str(args.filename)
    if args.tag is not None:
        sxglobals.tag = str(args.tag)
    if args.open is not None:
        sxglobals.catalogue_path = str(args.open)

    if sxglobals.catalogue_path is None:
        print('SX Node Manager: No Catalogue specified')

    sxglobals.source_files = get_source_files()
    sxglobals.nodes = load_nodes()

    if args.listonly and len(sxglobals.source_files) > 0:
        print('\nFound', len(sxglobals.source_files), 'source files:')
        for file in sxglobals.source_files:
            print(file)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--open', default=sxglobals.catalogue_path, help='Open a Catalogue file')
    parser.add_argument('-a', '--all', action='store_true', help='Export the entire Catalogue')
    parser.add_argument('-c', '--category', help='Export all objects in a category (Default, Paletted...')
    parser.add_argument('-f', '--filename', help='Export an object by filename')
    parser.add_argument('-t', '--tag', help='Export all tagged objects')
    parser.add_argument('-e', '--exportpath', default=sxglobals.export_path, help='Local folder where remote exports are collected to')
    parser.add_argument('-sd', '--subdivision', type=str, help='SX Tools subdivision override')
    parser.add_argument('-sp', '--palette', type=str, help='SX Tools palette override')
    parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='SX Tools flatten layers to VertexColor0')
    parser.add_argument('-l', '--listonly', action='store_true', help='Do not export, only list objects that match the other arguments')
    parser.add_argument('-u', '--updaterepo', action='store_true', help='Update art asset repositories on all nodes to the latest version (PlasticSCM)')
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
        print('SX Node Manager: Invalid file', file_path)
        return {}
    except IOError:
        print('SX Node Manager: File not found', file_path)
        return {}


def load_nodes():
    nodes = []
    if 'nodes' in sxglobals.conf.keys() and len(sxglobals.conf['nodes']) > 0:
        nodes_raw = sxglobals.conf['nodes']

        # Check if node is available
        for node in nodes_raw:
            if node['os'] == 'win':
                cmd_string = 'if exist %userprofile%\sxbatcher-blender\sx_batch_node.py (cd.) else (call)'
            else:
                cmd_string = 'test -f ~/sxbatcher-blender/sx_batch_node.py'

            available = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd_string], capture_output=True)
            if available.returncode == 0:
                nodes.append(node)

        if len(nodes) == 0:
            print('\nSX Node Manager: No network nodes')
        else:
            print('\nSX Node Manager: Active Nodes')
            for node in nodes:
                print('Node:', node['ip'], '\tCores:', node['numcores'], '\tOS:', node['os'])

    return nodes


def get_source_files():
    source_files = []
    asset_dict = {}

    if sxglobals.catalogue_path is not None:
        asset_dict = load_json(sxglobals.catalogue_path)

    if len(asset_dict) > 0:
        if sxglobals.all:
            for category in asset_dict.keys():
                for key in asset_dict[category].keys():
                    source_files.append(key)
        else:
            if sxglobals.category is not None:
                if sxglobals.category in asset_dict.keys():
                    for key in asset_dict[sxglobals.category].keys():
                        source_files.append(key)
            if sxglobals.filename is not None:
                for category in asset_dict.keys():
                    for key in asset_dict[category].keys():
                        if sxglobals.filename in key:
                            source_files.append(key)
            if sxglobals.tag is not None:
                for category in asset_dict.keys():
                    for key, values in asset_dict[category].items():
                        for value in values:
                            if sxglobals.tag == value:
                                source_files.append(key)
            if (sxglobals.category is None) and (sxglobals.filename is None) and (sxglobals.tag is None) and not sxglobals.updaterepo:
                print('SX Node Manager: Nothing selected for export')
    else:
        print('SX Node Manager: Invalid Catalogue')

    if len(source_files) > 0:
        source_files = list(set(source_files))
    elif (len(source_files) == 0) and (sxglobals.all or (sxglobals.category is not None) or (sxglobals.filename is not None) or (sxglobals.tag is not None)):
        print('\nSX Node Manager: No matching files in Catalogue')

    return source_files


def sx_update(update_task):
    p = subprocess.run(['ssh', update_task[0]+'@'+update_task[1], update_task[2]], text=True, capture_output=True)
    print(p.stdout)


def sx_batch(task):
    p0 = subprocess.run(['ssh', task[0]+'@'+task[1], task[2]], capture_output=True)
    p1 = subprocess.run(['ssh', task[0]+'@'+task[1], task[3]], text=True, capture_output=True)
    print(p1.stdout)


def sx_collect(collect_task):
    p = subprocess.run(['scp', '-r', collect_task[0]+'@'+collect_task[1]+':'+collect_task[2], collect_task[3]], text=True, capture_output=True)
    print('SX Node Manager: Results collected from', collect_task[1], 'to', collect_task[3])


def sx_cleanup(cleanup_task):
    p = subprocess.run(['ssh', cleanup_task[0]+'@'+cleanup_task[1], cleanup_task[2]], capture_output=True)


# ------------------------------------------------------------------------
#    NOTE: 1) The catalogue file should be located in the root
#          of your asset folder structure.
#
#          SX Node Manager expects sxbatcher-blender folder to be
#          located in user home folder. Adapt as necessary!
# ------------------------------------------------------------------------
sxglobals = sx_globals()

if __name__ == '__main__':
    init_globals()

    if len(sxglobals.nodes) > 0:

        # Task generation for Batch Node
        job_length = len(sxglobals.source_files)
        tasks = []
        i = 0
        while i < job_length:
            for j, node in enumerate(sxglobals.nodes):
                numcores = int(node['numcores'])
                nodefiles = sxglobals.source_files[i:(i + numcores)]

                if len(nodefiles) > 0:
                    if node['os'] == 'win':
                        cmd0 = 'mkdir %userprofile%\sx_batch_temp'
                        cmd1 = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py'
                        cmd1 += ' -e %userprofile%\sx_batch_temp -r'
                    else:
                        cmd0 = 'mkdir -p ~/sx_batch_temp'
                        cmd1 = 'python3 ~/sxbatcher-blender/sx_batch_node.py'
                        cmd1 += ' -e ~/sx_batch_temp/ -r'
                    for file in nodefiles:
                        cmd1 += ' '+file
                    if sxglobals.subdivision is not None:
                        cmd1 += ' -sd '+sxglobals.subdivision
                    if sxglobals.palette is not None:
                        cmd1 += ' -sp '+sxglobals.palette
                    if sxglobals.staticvertexcolors:
                        cmd1 += ' -st'

                    tasks.append((node['user'], node['ip'], cmd0, cmd1))
                i += numcores

        # Only collect and clean up nodes that have been tasked
        tasked_nodes = []
        for node in sxglobals.nodes:
            for task in tasks:
                if (task[1] == node['ip']) and (node not in tasked_nodes):
                    tasked_nodes.append(node)

        # Task for updating a version-controlled asset folder (currently using PlasticSCM)
        update_tasks = []
        for node in sxglobals.nodes:
            if node['os'] == 'win':
                upd_cmd = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py -u'
            else:
                upd_cmd = 'python3 ~/sxbatcher-blender/sx_batch_node.py -u'
            update_tasks.append((node['user'], node['ip'], upd_cmd))

        # Task for collecting generated FBX files from the node
        collect_tasks = []
        for node in tasked_nodes:
            if node['os'] == 'win':
                collection_path = '%userprofile%\sx_batch_temp\*'
            else:
                collection_path = '~/sx_batch_temp/*'

            collect_tasks.append((node['user'], node['ip'], collection_path, sxglobals.export_path))

        # Clean up afterwards, delete temp folder and contents
        cleanup_tasks = []
        for node in tasked_nodes:
            if node['os'] == 'win':
                clean_cmd = 'rmdir /Q /S %userprofile%\sx_batch_temp'
            else:
                clean_cmd = 'rm -rf ~/sx_batch_temp'
            cleanup_tasks.append((node['user'], node['ip'], clean_cmd))

        if sxglobals.updaterepo:
            print('\n'+'SX Node Manager: Updating Art Repositories')
            with Pool(processes=len(sxglobals.nodes)) as update_pool:
                update_pool.map(sx_update, update_tasks)

        if not sxglobals.listonly and (len(sxglobals.source_files) > 0):
            then = time.time()
            print('\n'+'SX Node Manager: Assigning Tasks')

            with Pool(processes=len(sxglobals.nodes)) as pool:
                pool.map(sx_batch, tasks)

            with Pool(processes=len(sxglobals.nodes)) as coll_pool:
                coll_pool.map(sx_collect, collect_tasks)

            with Pool(processes=len(sxglobals.nodes)) as cleanup_pool:
                cleanup_pool.map(sx_cleanup, cleanup_tasks)

            now = time.time()
            print('SX Node Manager: Export Finished!')
            print('Duration:', round(now-then, 2), 'seconds')
            print('Source Files Processed:', len(sxglobals.source_files))
