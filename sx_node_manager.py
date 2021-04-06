import argparse
import subprocess
import multiprocessing
import time
import json
import sys
from multiprocessing import Pool, Value, Array
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
        self.blender_path = None
        self.sxtools_path = None
        self.script_path = None
        self.catalogue_path = None
        self.export_path = None
        self.costs_path = None
        self.assets_path = None
        self.category = None
        self.filename = None
        self.tag = None
        self.all = None


def init_globals():
    sxglobals.conf = load_json(os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json'))
    sxglobals.blender_path = sxglobals.conf.get('blender_path')
    sxglobals.sxtools_path = sxglobals.conf.get('sxtools_path')
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
    sxglobals.benchmark = args.benchmark
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
    else:
        sxglobals.costs_path = sxglobals.catalogue_path.replace(os.path.basename(sxglobals.catalogue_path), 'sx_costs.json')
        if os.path.isfile(sxglobals.costs_path):
            sxglobals.source_costs = load_json(sxglobals.costs_path)

    sxglobals.script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
    sxglobals.assets_path = os.path.split(sxglobals.catalogue_path)[0].replace('//', os.path.sep)


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
    parser.add_argument('-b', '--benchmark', action='store_true', help='Calculate asset costs')
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


def save_json(file_path, data):
    with open(file_path, 'w') as output:
        temp_dict = {}
        temp_dict = data
        json.dump(temp_dict, output, indent=4)

        output.close()


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

    return nodes


def get_source_files(force_all=False):
    source_files = []
    asset_dict = {}

    if sxglobals.catalogue_path is not None:
        asset_dict = load_json(sxglobals.catalogue_path)

    if len(asset_dict) > 0:
        if sxglobals.all or force_all:
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
            if (sxglobals.category is None) and (sxglobals.filename is None) and (sxglobals.tag is None) and not sxglobals.updaterepo and not sxglobals.benchmark:
                print('SX Node Manager: Nothing selected for export')
    else:
        print('SX Node Manager: Invalid Catalogue')

    if len(source_files) > 0:
        source_files = list(set(source_files))
    elif (len(source_files) == 0) and (sxglobals.all or (sxglobals.category is not None) or (sxglobals.filename is not None) or (sxglobals.tag is not None)):
        print('\nSX Node Manager: No matching files in Catalogue')

    return source_files


def update_costs(force_all=False):
    def validation_check(file_paths, library):
        for file in file_paths:
            base = os.path.basename(file)
            if base not in library:
                return False
        return True

    if force_all or not validation_check(sxglobals.source_files, sxglobals.source_costs.keys()):
        tasks = []
        benchmark_files = []

        if force_all:
            source_files = get_source_files(force_all=True)
        else:
            source_files = sxglobals.source_files

        for file in source_files:
            file_path = file.replace('//', os.path.sep)
            benchmark_files.append(os.path.join(sxglobals.assets_path, file_path))

        if not force_all:
            for cost in sxglobals.source_costs.keys():
                filtered_sources = [file for file in benchmark_files if not cost in file]
                benchmark_files = filtered_sources[:]

        for file in benchmark_files:
            tasks.append((sxglobals.blender_path, file, sxglobals.script_path, sxglobals.export_path, sxglobals.sxtools_path))

        if force_all:
            print('SX Manager: Calculating asset costs')
        else:
            print('SX Manager: Cost values missing, updating sx_costs.json')

        results = multiprocessing.Array('d', [0.0]*len(benchmark_files))
        with Pool(processes=1, initializer=sx_init_benchmark, initargs=(results, tasks), maxtasksperchild=1) as costs_pool:
            costs_pool.map(sx_benchmark, range(len(tasks)))

        times = results[:]
        for i, file in enumerate(benchmark_files):
            sxglobals.source_costs[os.path.basename(file)] = times[i]
            print(file, times[i])

        save_json(sxglobals.costs_path, sxglobals.source_costs)


def sort_by_cost(source_files, cost_dict):
    cost_list = list(cost_dict.items())
    cost_list.sort(key = lambda x: x[1])

    sorted_source = []
    for cost in cost_list:
        for file in source_files:
            if cost[0] in file:
                sorted_source.append(file)

    return sorted_source


def sx_init_benchmark(result_array, task_array):
    global results, tasks
    results = result_array
    tasks = task_array


def sx_init_batch(done, tasknodes, nodes, sourcefiles, sourcecosts, subdiv, pal, stat):
    global processed, tasked_node_array
    processed = done
    tasked_node_array = tasknodes
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


def sx_benchmark(i):
    blender_path = tasks[i][0]
    source_file = tasks[i][1]
    script_path = tasks[i][2]
    export_path = tasks[i][3]
    sxtools_path = tasks[i][4]
    batch_args = [blender_path, "--background", "--factory-startup", "-noaudio", source_file, "--python", script_path, "--"]

    if export_path is not None:
        batch_args.extend(["-x", export_path])

    if sxtools_path is not None:
        batch_args.extend(["-l", sxtools_path])

    then = time.time()

    try:
        p = subprocess.run(batch_args, check=True, text=True, encoding='utf-8', capture_output=True)
    except subprocess.CalledProcessError as error:
        print('SX Benchmark Error:', source_file)

    now = time.time()
    results[i] = round(now-then, 2)


# Task for updating a version-controlled asset folder (currently using PlasticSCM)
def sx_update(node):
    if node['os'] == 'win':
        upd_cmd = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py -u'
    else:
        upd_cmd = 'python3 ~/sxbatcher-blender/sx_batch_node.py -u'

    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], upd_cmd], text=True, capture_output=True)
    print(p.stdout)


def sx_setup(node):
    if node['os'] == 'win':
        cmd = 'mkdir %userprofile%\sx_batch_temp'
    else:
        cmd = 'mkdir -p ~/sx_batch_temp'

    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd], capture_output=True)


def sx_batch(node):
    total_cores = 0
    node_idx = 0
    for i, sx_node in enumerate(sxglobals.nodes):
        total_cores += int(sx_node['numcores'])
        if node == sx_node:
            node_idx = i

    job_length = len(sxglobals.source_files)
    if processed.value < job_length:
        numcores = int(node['numcores'])
        work_amount = int(job_length * (numcores / float(total_cores)))
        if work_amount < numcores:
            work_amount = numcores
        if job_length - (processed.value + work_amount) <= 3:
            work_amount += (job_length - (processed.value + work_amount))
        batch_files = sxglobals.source_files[processed.value:(processed.value + work_amount)]
        processed.value = processed.value + work_amount

        print('Node:', '(' + node['os'] + ')', node['ip'], '\tBatch Size:', len(batch_files), '\tCores:', numcores, '\tLog:', 'sx_export_log_' + node['ip'].replace('.', '') + '.txt')

        if len(batch_files) > 0:
            tasked_node_array[node_idx] = 1
            if node['os'] == 'win':
                cmd = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py'
                cmd += ' -e %userprofile%\sx_batch_temp -r'
            else:
                cmd = 'python3 ~/sxbatcher-blender/sx_batch_node.py'
                cmd += ' -e ~/sx_batch_temp/ -r'
            for file in batch_files:
                cmd += ' '+file
            if sxglobals.subdivision is not None:
                cmd += ' -sd '+sxglobals.subdivision
            if sxglobals.palette is not None:
                cmd += ' -sp '+sxglobals.palette
            if sxglobals.staticvertexcolors:
                cmd += ' -st'

            then = time.time()
            with open('sx_export_log_' + node['ip'].replace('.', '') + '.txt', 'ab') as out:
                p = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd], text=True, stdout=out, stderr=subprocess.STDOUT)
            now = time.time()
            print('SX Node Manager:', node['ip'], 'completed in', round(now-then, 2), 'seconds')


def sx_cost_batch(node):
    # Examine job costs
    total_cores = 0
    node_idx = 0
    for i, sx_node in enumerate(sxglobals.nodes):
        total_cores += int(sx_node['numcores'])
        if node == sx_node:
            node_idx = i

    cost_list = list(sxglobals.source_costs.items())
    cost_list.sort(key = lambda x: x[1])
    cost_list.reverse()

    total_cost = 0
    for cost in cost_list:
        total_cost += cost[1]

    # Allocate (and adjust) work share bias
    batch_dict = {}
    work_shares = [0] * len(sxglobals.nodes)
    start = 0
    for j, sx_node in enumerate(sxglobals.nodes):
        batch_files = []
        numcores = int(sx_node['numcores'])
        bias = 0
        work_load = 0
        work_share = total_cost * ((float(numcores) + bias) / (float(total_cores) + (len(sxglobals.nodes) * bias)))

        for k in range(start, len(sxglobals.source_files)):
            if (work_load + cost_list[k][1]) < work_share:
                batch_files.append(sxglobals.source_files[k])
                work_load += cost_list[k][1]
                start += 1
            elif j+1 == len(sxglobals.nodes):
                batch_files.append(sxglobals.source_files[k])
                work_load += cost_list[k][1]
                start += 1

        batch_dict[j] = batch_files[:]
        work_shares[j] = work_share

    print('NodeID:', node_idx, '(' + node['os'] + ')', node['ip'], '\tWork Share:', str(round((work_shares[node_idx] / total_cost) * 100, 0))+'%', '(' + str(len(batch_dict[node_idx])) + ' files)', '\tCores:', node['numcores'], '\tLog:', 'sx_export_log_' + node['ip'].replace('.', '') + '.txt')

    if len(batch_dict[node_idx]) > 0:
        tasked_node_array[node_idx] = 1
        if node['os'] == 'win':
            cmd = 'python %userprofile%\sxbatcher-blender\sx_batch_node.py'
            cmd += ' -e %userprofile%\sx_batch_temp -r'
        else:
            cmd = 'python3 ~/sxbatcher-blender/sx_batch_node.py'
            cmd += ' -e ~/sx_batch_temp/ -r'
        for file in batch_dict[node_idx]:
            cmd += ' '+file
        if sxglobals.subdivision is not None:
            cmd += ' -sd '+sxglobals.subdivision
        if sxglobals.palette is not None:
            cmd += ' -sp '+sxglobals.palette
        if sxglobals.staticvertexcolors:
            cmd += ' -st'

        then = time.time()
        with open('sx_export_log_' + node['ip'].replace('.', '') + '.txt', 'ab') as out:
            p = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd], text=True, stdout=out, stderr=subprocess.STDOUT)
        now = time.time()
        print('SX Node Manager:', node['ip'], 'completed in', round(now-then, 2), 'seconds')


def sx_collect(node):
    if node['os'] == 'win':
        collection_path = '%userprofile%\sx_batch_temp\*'
    else:
        collection_path = '~/sx_batch_temp/*'

    p = subprocess.run(['scp', '-r', node['user']+'@'+node['ip']+':'+collection_path, sxglobals.export_path], text=True, capture_output=True)
    print('SX Node Manager: Results collected from', node['ip'], 'to', sxglobals.export_path)


def sx_cleanup(node):
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
    sxglobals.nodes = get_nodes()

    tasked_node_array = multiprocessing.Array('i', [0]*len(sxglobals.nodes))
    processed = multiprocessing.Value('i', 0)

    # Update or calculate asset costs if sx_costs.json is in use
    if sxglobals.benchmark:
        print('ben')
        update_costs(force_all=True)

    if (sxglobals.source_costs is not None) and not sxglobals.benchmark:
        update_costs(force_all=False)
        files_by_cost = sort_by_cost(sxglobals.source_files, sxglobals.source_costs)
        sxglobals.source_files = files_by_cost[::-1]


    # Display files that match the arguments
    if sxglobals.listonly and len(sxglobals.source_files) > 0:
        print('\nFound', len(sxglobals.source_files), 'source files:')
        for file in sxglobals.source_files:
            print(file)

    # Network-distributed tasks
    if len(sxglobals.nodes) > 0 and not sxglobals.benchmark:
        if sxglobals.updaterepo:
            print('\n'+'SX Node Manager: Updating Art Repositories')
            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.nodes, None), maxtasksperchild=1) as update_pool:
                update_pool.map(sx_update, sxglobals.nodes)

        if not sxglobals.listonly and (len(sxglobals.source_files) > 0):
            then = time.time()

            # 1) Create temp folders
            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.nodes, None), maxtasksperchild=1) as setup_pool:
                setup_pool.map(sx_setup, sxglobals.nodes)

            # 2) Process file batches
            if sxglobals.source_costs is not None:
                print('\n'+'SX Node Manager: Assigning Tasks (cost-based method)')
                task_type = sx_cost_batch
            else:
                print('\n'+'SX Node Manager: Assigning Tasks (default method)')
                task_type = sx_batch

            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_batch, initargs=(processed, tasked_node_array, sxglobals.nodes, sxglobals.source_files, sxglobals.source_costs, sxglobals.subdivision, sxglobals.palette, sxglobals.staticvertexcolors), maxtasksperchild=1) as batch_pool:
                batch_pool.map(task_type, sxglobals.nodes)

            # 3) Only collect from nodes that have been tasked
            sxglobals.tasked_nodes = []
            for i, node in enumerate(sxglobals.nodes):
                if tasked_node_array[i] == 1:
                    sxglobals.tasked_nodes.append(node)

            with Pool(processes=len(sxglobals.tasked_nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.tasked_nodes, sxglobals.export_path), maxtasksperchild=1) as collect_pool:
                collect_pool.map(sx_collect, sxglobals.tasked_nodes)

            # 4) Clean up nodes
            with Pool(processes=len(sxglobals.nodes), initializer=sx_init_housekeeping, initargs=(sxglobals.tasked_nodes, None), maxtasksperchild=1) as cleanup_pool:
                cleanup_pool.map(sx_cleanup, sxglobals.nodes)

            now = time.time()
            print('SX Node Manager: Export Finished!')
            print('Duration:', round(now-then, 2), 'seconds')
            print('Source Files Processed:', len(sxglobals.source_files))
