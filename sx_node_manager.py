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
        self.source_costs = None
        self.nodes = None
        self.tasked_nodes = None
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

    costs_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_costs.json')
    if os.path.isfile(costs_path):
        sxglobals.source_costs = load_json(costs_path)


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


def get_nodes():
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


def sort_by_cost(source_files, cost_dict):
    cost_list = list(cost_dict.items())
    cost_list.sort(key = lambda x: x[1])

    sorted_source = []
    for cost in cost_list:
        for file in source_files:
            if cost[0] in file:
                sorted_source.append(file)

    return sorted_source


def sx_init_batch(done, tasknodes, sourcelocks, nodes, sourcefiles, sourcecosts, subdiv, pal, stat):
    global processed, source_lock_array, tasked_node_array
    processed = done
    tasked_node_array = tasknodes
    source_lock_array = sourcelocks
    sxglobals.nodes = nodes
    sxglobals.source_files = sourcefiles
    sxglobals.source_costs = sourcecosts
    sxglobals.subdivision = subdiv
    sxglobals.palette = pal
    sxglobals.staticvertexcolors = stat


def sx_init_housekeeping(nodes, ep):
    sxglobals.nodes = nodes
    sxglobals.tasked_nodes = nodes
    sxglobals.export_path = ep


# Task for updating a version-controlled asset folder (currently using PlasticSCM)
def sx_update(i):
    node = sxglobals.nodes[i]
    if node['os'] == 'win':
        upd_cmd = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py -u'
    else:
        upd_cmd = 'python3 ~/sxbatcher-blender/sx_batch_node.py -u'

    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], upd_cmd], text=True, capture_output=True)
    print(p.stdout)


def sx_batch(i):
    total_cores = 0
    for node in sxglobals.nodes:
        total_cores += int(node['numcores'])

    job_length = len(sxglobals.source_files)
    if processed.value < job_length:
        node = sxglobals.nodes[i]
        numcores = int(node['numcores'])
        work_amount = int(job_length * (numcores / float(total_cores)))
        if work_amount < numcores:
            work_amount = numcores
        if job_length - (processed.value + work_amount) <= 3:
            work_amount += (job_length - (processed.value + work_amount))
        batch_files = sxglobals.source_files[processed.value:(processed.value + work_amount)]
        processed.value = processed.value + work_amount

        if len(batch_files) > 0:
            tasked_node_array[i] = 1
            if node['os'] == 'win':
                cmd0 = 'mkdir %userprofile%\sx_batch_temp'
                cmd1 = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py'
                cmd1 += ' -e %userprofile%\sx_batch_temp -r'
            else:
                cmd0 = 'mkdir -p ~/sx_batch_temp'
                cmd1 = 'python3 ~/sxbatcher-blender/sx_batch_node.py'
                cmd1 += ' -e ~/sx_batch_temp/ -r'
            for file in batch_files:
                cmd1 += ' '+file
            if sxglobals.subdivision is not None:
                cmd1 += ' -sd '+sxglobals.subdivision
            if sxglobals.palette is not None:
                cmd1 += ' -sp '+sxglobals.palette
            if sxglobals.staticvertexcolors:
                cmd1 += ' -st'

        p0 = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd0], capture_output=True)
        p1 = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd1], text=True, capture_output=True)
        print(p1.stdout)


def sx_collect(i):
    node = sxglobals.tasked_nodes[i]
    if node['os'] == 'win':
        collection_path = '%userprofile%\sx_batch_temp\*'
    else:
        collection_path = '~/sx_batch_temp/*'

    p = subprocess.run(['scp', '-r', node['user']+'@'+node['ip']+':'+collection_path, sxglobals.export_path], text=True, capture_output=True)
    print('SX Node Manager: Results collected from', node['ip'], 'to', sxglobals.export_path)


def sx_cleanup(i):
    node = sxglobals.tasked_nodes[i]
    if node['os'] == 'win':
        clean_cmd = 'rmdir /Q /S %userprofile%\sx_batch_temp'
    else:
        clean_cmd = 'rm -rf ~/sx_batch_temp'

    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], clean_cmd], capture_output=True)


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
    sxglobals.source_files = get_source_files()
    if sxglobals.source_costs is not None:
        files_by_cost = sort_by_cost(sxglobals.source_files, sxglobals.source_costs)
        sxglobals.source_files = files_by_cost[::-1]
    sxglobals.nodes = get_nodes()

    if sxglobals.listonly and len(sxglobals.source_files) > 0:
        print('\nFound', len(sxglobals.source_files), 'source files:')
        for file in sxglobals.source_files:
            print(file)

    if len(sxglobals.nodes) > 0:
        if sxglobals.updaterepo:
            print('\n'+'SX Node Manager: Updating Art Repositories')
            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.nodes, None), maxtasksperchild=1) as update_pool:
                update_pool.map(sx_update, range(len(sxglobals.nodes)))

        if not sxglobals.listonly and (len(sxglobals.source_files) > 0):
            tasked_node_array = multiprocessing.Array('i', [0]*len(sxglobals.nodes))
            source_lock_array = multiprocessing.Array('i', [0]*len(sxglobals.source_files))
            processed = multiprocessing.Value('i', 0)

            then = time.time()
            print('\n'+'SX Node Manager: Assigning Tasks')

            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_batch, initargs=(processed, tasked_node_array, source_lock_array, sxglobals.nodes, sxglobals.source_files, sxglobals.source_costs, sxglobals.subdivision, sxglobals.palette, sxglobals.staticvertexcolors), maxtasksperchild=1) as batch_pool:
                batch_pool.map(sx_batch, range(len(sxglobals.nodes)))

            # Only collect and clean up nodes that have been tasked
            sxglobals.tasked_nodes = []
            for i, node in enumerate(sxglobals.nodes):
                if tasked_node_array[i] == 1:
                    sxglobals.tasked_nodes.append(node)

            with Pool(processes=len(sxglobals.tasked_nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.tasked_nodes, sxglobals.export_path), maxtasksperchild=1) as collect_pool:
                collect_pool.map(sx_collect, range(len(sxglobals.tasked_nodes)))

            with Pool(processes=len(sxglobals.tasked_nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.tasked_nodes, None), maxtasksperchild=1) as cleanup_pool:
                cleanup_pool.map(sx_cleanup, range(len(sxglobals.tasked_nodes)))

            now = time.time()
            print('SX Node Manager: Export Finished!')
            print('Duration:', round(now-then, 2), 'seconds')
            print('Source Files Processed:', len(sxglobals.source_files))
