import threading
import subprocess
import multiprocessing
import time
import json
import socket
import pathlib
import struct
import os
import platform
import tkinter as tk
from tkinter import MULTIPLE, ttk
from tkinter import filedialog


# ------------------------------------------------------------------------
#    Globals
# ------------------------------------------------------------------------
class SXBATCHER_globals(object):
    def __init__(self):
        # Main locations, validate if changed
        self.blender_path = None
        self.catalogue_path = None
        self.export_path = None
        self.sxtools_path = None
        self.asset_path = None

        self.num_cores = None
        self.share_cpus = None
        self.shared_cores = None
        self.use_network_nodes = None
        self.ip_addr = None
        self.nodename = None
        self.catalogue = None
        self.categories = None
        self.category = None
        self.then = None
        self.now = None

        self.update_repo = False # call all nodes to update their plastic repos
        self.listonly = None # only show files that contain export objs
        self.all = None # if full catalogue or all objects are to be exported
        self.script_path = None

        # Batch lists
        self.export_objs = None
        self.source_files = None
        self.source_costs = None
        self.remote_assignment = []
        self.errors = []
        self.revision_dict = {}

        # Blender setting overrides
        self.debug = False
        self.palette = False
        self.palette_name = None
        self.subdivision = False
        self.subdivision_count = None
        self.static_vertex_colors = False
        self.revision_export = False

        # Network settings
        self.group = '239.1.1.1'
        self.discovery_port = 50000
        self.file_transfer_port = 50001
        self.magic = 'fna349fn'
        self.magic_task = 'snaf68yh'
        self.nodes = []
        self.tasked_nodes = None
        self.node_busy_status = False

        return None


# ------------------------------------------------------------------------
#    Initialization and I/O
# ------------------------------------------------------------------------
class SXBATCHER_init(object):
    def __init__(self):
        conf = self.load_conf()
        sxglobals.blender_path = conf.get('blender_path')
        sxglobals.catalogue_path = conf.get('catalogue_path')
        sxglobals.export_path = conf.get('export_path')
        sxglobals.sxtools_path = conf.get('sxtools_path')
        sxglobals.debug = bool(int(conf.get('debug', False)))
        sxglobals.palette = bool(int(conf.get('palette', False)))
        sxglobals.palette_name = conf.get('palette_name', '')
        sxglobals.subdivision = bool(int(conf.get('subdivision', False)))
        sxglobals.subdivision_count = int(conf.get('subdivision_count', 0))
        sxglobals.static_vertex_colors = bool(int(conf.get('static_vertex_colors', False)))
        sxglobals.revision_export = bool(int(conf.get('revision_export', False)))
        sxglobals.share_cpus = bool(int(conf.get('share_cpus', False)))
        sxglobals.shared_cores = int(conf.get('shared_cores', 0))
        sxglobals.use_network_nodes = bool(int(conf.get('use_nodes', False)))
        self.validate_paths()

        sxglobals.script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
        if sxglobals.catalogue_path is not None:
            sxglobals.asset_path = os.path.split(sxglobals.catalogue_path)[0].replace('//', os.path.sep)

        sxglobals.ip_addr = self.get_ip()
        sxglobals.nodename = 'Node '+sxglobals.ip_addr
        sxglobals.num_cores = multiprocessing.cpu_count()

        if sxglobals.catalogue_path is None:
            sxglobals.catalogue = {'empty': {'empty':{'objects':['empty', ], 'tags':['empty', ]}}}
        else:
            sxglobals.catalogue = self.load_asset_data(sxglobals.catalogue_path)
        sxglobals.categories = list(sxglobals.catalogue.keys())
        sxglobals.category = sxglobals.categories[0]

        return None


    def get_ip(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(('10.255.255.255', 1))
            ip = s.getsockname()[0]
        except Exception:
            ip = '127.0.0.1'
        finally:
            s.close()
        return ip


    def load_json(self, file_path):
        try:
            with open(file_path, 'r') as input:
                temp_dict = {}
                temp_dict = json.load(input)
                input.close()
            return temp_dict
        except ValueError:
            print(sxglobals.nodename + ' Error: Invalid JSON file. ' + file_path)
            return {}
        except IOError:
            return {}


    def save_json(self, file_path, data):
        with open(file_path, 'w') as output:
            temp_dict = {}
            temp_dict = data
            json.dump(temp_dict, output, indent=4)
            output.close()


    def load_conf(self):
        conf = {}
    
        if os.path.isfile(os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')):
            conf_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')
            conf = self.load_json(conf_path)

        return conf


    def save_conf(self):
        conf_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')
        temp_dict = {}
        temp_dict['blender_path'] = sxglobals.blender_path.replace(os.path.sep, '//')
        temp_dict['catalogue_path'] = sxglobals.catalogue_path.replace(os.path.sep, '//')
        temp_dict['export_path'] = sxglobals.export_path.replace(os.path.sep, '//')
        temp_dict['sxtools_path'] = sxglobals.sxtools_path.replace(os.path.sep, '//')
        temp_dict['debug'] = str(int(sxglobals.debug))
        temp_dict['palette'] = str(int(sxglobals.palette))
        temp_dict['palette_name'] = sxglobals.palette_name
        temp_dict['subdivision'] = str(int(sxglobals.subdivision))
        temp_dict['subdivision_count'] = str(sxglobals.subdivision_count)
        temp_dict['static_vertex_colors'] = str(int(sxglobals.static_vertex_colors))
        temp_dict['revision_export'] = str(int(sxglobals.revision_export))
        temp_dict['share_cpus'] = str(int(sxglobals.share_cpus))
        temp_dict['shared_cores'] = str(int(sxglobals.shared_cores))
        temp_dict['use_nodes'] = str(int(sxglobals.use_network_nodes))

        self.save_json(conf_path, temp_dict)
        print('SX Batcher: ' + conf_path + ' saved')


    def validate_paths(self):
        if sxglobals.blender_path is not None:
            sxglobals.blender_path = sxglobals.blender_path.replace('//', os.path.sep) if os.path.isfile(sxglobals.blender_path.replace('//', os.path.sep)) else None
        if sxglobals.catalogue_path is not None:
            sxglobals.catalogue_path = sxglobals.catalogue_path.replace('//', os.path.sep) if os.path.isfile(sxglobals.catalogue_path.replace('//', os.path.sep)) else None
        if sxglobals.export_path is not None:
            sxglobals.export_path = sxglobals.export_path.replace('//', os.path.sep) if os.path.isdir(sxglobals.export_path.replace('//', os.path.sep)) else None
        if sxglobals.sxtools_path is not None:
            sxglobals.sxtools_path = sxglobals.sxtools_path.replace('//', os.path.sep) if os.path.isdir(sxglobals.sxtools_path.replace('//', os.path.sep)) else None

        return (sxglobals.blender_path is not None) and (sxglobals.catalogue_path is not None) and (sxglobals.export_path is not None) and (sxglobals.sxtools_path is not None)


    def load_asset_data(self, catalogue_path):
        if os.path.isfile(catalogue_path):
            return self.load_json(catalogue_path)
        else:
            print(sxglobals.nodename + ': Invalid Catalogue path')
            return {'empty': {'empty':{'objects':['empty', ], 'tags':['empty', ]}}}


    def transfer_files(self, address, files):
        bufsize = 4096
        print(files)
        # sizemap should be
        # filename:size
        # doesn't matter how you get there
        # below is a placeholder
        sizemap = { file.name:file.stat().st_size for file in files}
        print(f'[+] files to be transferred:')
        for file, size in sizemap.items():
            print(f'\t{file}: {size}')
        # open TCP socket in context manager
        with socket.create_connection(address, timeout=60) as sock:
            # send file:size json
            sock.send(json.dumps(sizemap).encode('utf-8'))
            time.sleep(0.1)
            # for each file, read as binary and shove into socket
            for file in files:
                with open(file, 'rb') as f:
                    print(f'[+] transfering {file}... ', end='')
                    while chunk := f.read(bufsize):
                        sock.send(chunk)
                print('done')

        # path = pathlib.Path('C:/Users/zephyrus/dummy_data')
        # all files in folder
        # for_transfer = [file for file in path.iterdir()]
        # hit it
        # transfer_files(('127.0.0.1', 5158), for_transfer)


    def receive_files(self, address):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # bind to given address and wait
            sock.bind(address)
            sock.listen()
            """ accept() returns a new socket 
            for some unfathomable reason """
            conn, addr = sock.accept()
            print(f'[+] got connection {addr}')
            metadata = json.loads(conn.recv(bufsize).decode('utf-8'))
            
            # directory to save in, change this
            target_dir = pathlib.Path.home() / 'receivetest'
            target_dir.mkdir(exist_ok=True)

            metadata = {pathlib.Path(key).name:int(val) for key, val in metadata.items()}
            print(f'[+] got file list:')
            for fname in metadata:
                print(f'\t{fname}')
            for file, size in metadata.items():
                with open(target_dir / file, 'wb') as f:
                    print(f'[+] writing into {file}...', end='')
                    # check remaining data size is more than zero
                    # read bufsize if there's more than bufsize left
                    # else modulo
                    left = size
                    while left:
                        quot, remain = divmod(left, bufsize)
                        left -= f.write(conn.recv(bufsize if quot else remain))
                        
                    print(f' {f.tell()}/{size}')
            conn.close()

        # receive_files(('127.0.0.1', 5158))


# ------------------------------------------------------------------------
#    Batch Manager for Localhost and Nodes
# ------------------------------------------------------------------------
class SXBATCHER_batch_manager(object):
    def __init__(self):
        return None


    def get_source_assets(self, revisions_only=False):
        revision_path = sxglobals.export_path + os.path.sep + 'file_revisions.json'
        current_revisions = init.load_json(revision_path)
        new_revisions = {}

        # Also update catalogue in case of new revisions
        sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)
        sxglobals.categories = list(sxglobals.catalogue.keys())
        sxglobals.category = sxglobals.categories[0]

        source_assets = []
        for obj in sxglobals.export_objs:
            for category in sxglobals.catalogue:
                for asset, obj_dict in sxglobals.catalogue[category].items():
                    if obj in obj_dict['objects']:
                        if revisions_only:
                            revision = obj_dict.get('revision', str(0))
                            if (asset not in current_revisions.keys()):
                                new_revisions[asset] = revision
                                source_assets.append((asset, int(obj_dict['cost'])))
                            elif (asset in current_revisions.keys()) and (int(current_revisions[asset]) < int(revision)):
                                new_revisions[asset] = revision
                                source_assets.append((asset, int(obj_dict['cost'])))
                            else:
                                print('SX Batcher: No changes in', asset)
                        else:
                            source_assets.append((asset, int(obj_dict['cost'])))

        source_assets = list(set(source_assets))

        # current_revisions.update(new_revisions)
        # init.save_json(revision_path, current_revisions)

        source_assets.sort(key = lambda x: x[1], reverse=True)
        source_files = []
        for asset in source_assets:
            source_files.append(asset[0])
        return source_files


    def get_source_revisions(self):
        source_assets = []
        for obj in sxglobals.export_objs:
            for category in sxglobals.catalogue:
                for filepath, obj_dict in sxglobals.catalogue[category].items():
                    if obj in obj_dict['objects']:
                        source_assets.append((filepath, obj_dict.get('revision', str(0))))

        source_assets = list(set(source_assets))
        revision_dict = {}
        for asset in source_assets:
            revision_dict[asset[0]] = asset[1]
        return revision_dict


    def update_revisions(self):
        file_path = sxglobals.export_path + os.path.sep + 'file_revisions.json'
        file_dict = init.load_json(file_path)
        data_dict = self.get_source_revisions()
        file_dict.update(data_dict)
        init.save_json(file_path, file_dict)


    # Handles task assignments:
    # 1) Local-only batch processing assigned via GUI
    # 2) Distributed batch processing assigned via GUI
    # 3) Work batches assigned by a remote node
    def task_handler(self, remote_task=False):
        sxglobals.export_objs = []
        gui.label_progress.configure(text='Job Running')
        sxglobals.then = time.perf_counter()
        for i in range(gui.lb_export.size()):
            sxglobals.export_objs.append(gui.lb_export.get(i))

        if remote_task:
            # Receive files to be processed from network node
            if sxglobals.share_cpus and (len(sxglobals.remote_assignment) > 0):
                tasks = self.prepare_received_tasks()
                t = threading.Thread(target=batch_local.worker_spawner, args=(tasks, sxglobals.shared_cores))
                t.start()
                gui.step_check(t)
        else:
            if sxglobals.use_network_nodes:
                # Send files to be processed to network nodes
                node_tasks = self.prepare_node_tasks()
                if len(node_tasks) > 0:
                    for node_ip, task_list in node_tasks.items():
                        # Submit files to node
                        source_files = []
                        for task in task_list:
                            file_path = task['asset'].replace('//', os.path.sep)
                            source_files.append(pathlib.Path(os.path.join(sxglobals.asset_path, file_path)))
                            task['asset'] = os.path.basename(task['asset'])
                        if len(source_files) > 0:
                            init.transfer_files((node_ip, sxglobals.file_transfer_port), source_files)

                        # Send task list to node
                    """
                        for value in node_tasks[key]:
                            payload = json.dumps(value).encode('utf-8')
                            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
                            addr = sxglobals.nodes[key][0]
                            port = sxglobals.discovery_port
                            sock.sendto(payload, (addr, port))
                            if __debug__:
                                print("sent: {}".format(payload))
                            sock.close()
                    """
            else:
                # Receive export list created in the UI
                tasks = self.prepare_local_tasks()
                if len(tasks) > 0:
                    t = threading.Thread(target=batch_local.worker_spawner, args=(tasks, sxglobals.num_cores))
                    t.start()
                    gui.step_check(t)
                else:
                    gui.label_progress.configure(text='No Changes!')
                    gui.button_batch['state'] = 'normal'
                    gui.progress_bar['value'] = 0
                    sxglobals.errors = []


    def prepare_local_tasks(self):
        # grab blender work script from the location of this script
        script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
        asset_path = os.path.split(sxglobals.catalogue_path)[0].replace('//', os.path.sep)
        subdivision = None
        palette = None

        # check Blender override flags
        if sxglobals.subdivision:
            subdivision = str(sxglobals.subdivision_count)
        if sxglobals.palette:
            palette = sxglobals.palette_name

        # get asset paths from catalogue, map to file system locations, remove doubles
        source_assets = self.get_source_assets(sxglobals.revision_export)
        export_path = os.path.abspath(sxglobals.export_path)

        source_files = []
        for asset in source_assets:
            file_path = asset.replace('//', os.path.sep)
            source_files.append(os.path.join(asset_path, file_path))
        if len(source_files) > 0:
            print('\n' + sxglobals.nodename + ': Source files:')
            for file in source_files:
                print(file)

        # Generate task definition for each local headless Blender
        tasks = []
        for file in source_files:
            tasks.append(
                (sxglobals.blender_path,
                file,
                script_path,
                os.path.abspath(export_path),
                os.path.abspath(sxglobals.sxtools_path),
                subdivision,
                palette,
                sxglobals.static_vertex_colors,
                sxglobals.debug))

        return tasks


    def prepare_received_tasks(self):
        # grab blender work script from the location of this script
        script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
        asset_path = os.path.realpath('batch_submissions')

        remote_tasks = sxglobals.remote_assignment
        tasks = []
        for remote_task in remote_tasks:
            subdivision_count = None
            palette_name = None
            asset = remote_task['asset']
            subdivision = remote_task['subdivision']
            if subdivision:
                subdivision_count = str(remote_task['subdivision_count'])
            palette = remote_task['palette']
            if palette:
                palette_name = remote_task['palette_name']
            static_vertex_colors = remote_task['static_vertex_colors']
            debug = remote_task['debug']

            # create results folder
            dir_name = 'batch_results'
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)
                print("Folder" , dir_name,  "created")
            else:    
                print("Folder" , dir_name,  "exists")

            export_path = str(os.path.realpath(dir_name))

            file = os.path.join(asset_path, asset)
            print(file)

            # Generate task definition for each local headless Blender
            tasks.append(
                (sxglobals.blender_path,
                file,
                script_path,
                os.path.abspath(export_path),
                os.path.abspath(sxglobals.sxtools_path),
                subdivision_count,
                palette_name,
                static_vertex_colors,
                debug))

        return tasks


    def prepare_node_tasks(self):
        source_assets = self.get_source_assets(sxglobals.revision_export)
        tasks = []

        for asset in source_assets:
            tasks.append({
                "magic": sxglobals.magic_task,
                "asset": asset,
                "subdivision": str(sxglobals.subdivision),
                "subdivision_count": str(sxglobals.subdivision_count),
                "palette": str(sxglobals.palette),
                "palette_name": sxglobals.palette_name,
                "static_vertex_colors": str(sxglobals.static_vertex_colors),
                "debug": str(sxglobals.debug),
                "batch_size": str(len(source_assets))
            })

        # Divide tasks per node according to core counts
        workload = len(tasks)
        node_tasks = {}
        for node in sxglobals.nodes:
            node_ip = node[0]
            node_tasks[node_ip] = []

        i = 0
        while i < workload:
            for node in sxglobals.nodes:
                node_ip = node[0]
                cores = node[3]
                for j in range(int(cores)):
                    if (j+i) < workload:
                        task_list = node_tasks[node_ip]
                        task_list.append(tasks[j+i])
                        node_tasks[node_ip] = task_list
                    else:
                        break
                i += j

        return node_tasks


# ------------------------------------------------------------------------
#    Local Multiprocessing
# ------------------------------------------------------------------------
class SXBATCHER_batch_local(object):
    def __init__(self):
        return None


    def worker_process(self, process_args):
        blender_path = process_args[0]
        source_file = process_args[1]
        script_path = process_args[2]
        export_path = process_args[3]
        sxtools_path = process_args[4]
        subdivision = process_args[5]
        palette = process_args[6]
        staticvertexcolors = process_args[7]
        debug = process_args[8]

        batch_args = [blender_path, "--background", "--factory-startup", "-noaudio", source_file, "--python", script_path]

        if debug:
            batch_args.extend(["-d"])

        batch_args.extend(["--"])

        batch_args.extend(["-x", export_path])

        batch_args.extend(["-l", sxtools_path])

        if subdivision is not None:
            batch_args.extend(["-sd", subdivision])

        if palette is not None:
            batch_args.extend(["-sp", palette])

        if staticvertexcolors:
            batch_args.extend(["-st"])

        try:
            p = subprocess.run(batch_args, check=True, text=True, encoding='utf-8', capture_output=True)
            # For debugging add "-d" to batch args and remove the keyword filter
            lines = p.stdout.splitlines()
            counter = 0
            for line in lines:
                if debug:
                    if 'clnors' not in line:
                        print(line)
                else:
                    if 'Error' in line:
                        counter = 10
                        print(line)
                        return (source_file)
                    if counter > 0:
                        print(line)
                        counter -= 1
        except subprocess.CalledProcessError as error:
            print('SX Batcher Error: Blender process crashed')
            return (source_file)


    def worker_spawner(self, tasks, num_cores):
        print(sxglobals.nodename + ': Spawning workers')

        with multiprocessing.Pool(processes=num_cores, maxtasksperchild=1) as pool:
            for i, error in enumerate(pool.imap(self.worker_process, tasks)):
                gui.progress_bar['value'] = round(i/len(tasks)*100)
                if error is not None:
                    sxglobals.errors.append(error)

        sxglobals.now = time.perf_counter()
        print(sxglobals.nodename + ':', len(sxglobals.export_objs), 'objects exported in', round(sxglobals.now-sxglobals.then, 2), 'seconds\n')
        if len(sxglobals.errors) > 0:
            print(sxglobals.nodename + ': Errors in:')
            for file in sxglobals.errors:
                print(file)


# ------------------------------------------------------------------------
#    Network Distributed Processing
# ------------------------------------------------------------------------
class SXBATCHER_batch_nodes(object):

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

                then = time.perf_counter()
                with open('sx_export_log_' + node['ip'].replace('.', '') + '.txt', 'ab') as out:
                    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd], text=True, stdout=out, stderr=subprocess.STDOUT)
                now = time.perf_counter()
                print('SX Node Manager:', node['ip'], 'completed in', round(now-then, 2), 'seconds')


    def sx_cost_batch(node):

        def validate_lists(source_list, cost_list):
            test_list = source_list[:]
            for cost in cost_list:
                for i, test in enumerate(test_list):
                    if cost[0] == os.path.basename(test):
                        test_list[i] = ''
                        break

            for test in test_list:
                if test != '':
                    print('SX Node Manager Error: Multiple instances in Catalogue', test)
                    return False
            else:
                return True

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

        if validate_lists(sxglobals.source_files, cost_list):
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

                then = time.perf_counter()
                with open('sx_export_log_' + node['ip'].replace('.', '') + '.txt', 'ab') as out:
                    p = subprocess.run(['ssh', node['user']+'@'+node['ip'], cmd], text=True, stdout=out, stderr=subprocess.STDOUT)
                now = time.perf_counter()
                print('SX Node Manager:', node['ip'], 'completed in', round(now-then, 2), 'seconds')


# ------------------------------------------------------------------------
#    Network Node Broadcasting
#    Responsible for broadcasting availability of CPU resources
# ------------------------------------------------------------------------
class SXBATCHER_node_broadcast_thread(threading.Thread):
    def __init__(self, payload, group, port, timeout=5):
        super().__init__()
        self.stop_event = threading.Event()
        self.payload = json.dumps(payload).encode('utf-8')
        self.group = group
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)


    def stop(self):
        self.stop_event.set()
        self.sock.close()


    def run(self):
        while not self.stop_event.wait(timeout=5):
            self.sock.sendto(self.payload, (self.group, self.port))
            if __debug__:
                print("sent: {}".format(self.payload))


# ------------------------------------------------------------------------
#    Network Node Discovery
#    Runs on host, receives multicast broadcasts from available nodes
# ------------------------------------------------------------------------
class SXBATCHER_node_discovery_thread(threading.Thread):
    def __init__(self, group, port, timeout=5):
        super().__init__()
        self.stop_event = threading.Event()
        self.group = group
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(5)
        self.sock.bind(('', self.port))
        packed = struct.pack('=4sl', socket.inet_aton(self.group), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, packed)


    def stop(self):
        self.stop_event.set()
        self.sock.close()


    def run(self):
        # while not self.stop_event.wait(timeout=5):
        while not self.stop_event.is_set():
            try:
                received, address = self.sock.recvfrom(1024)
                fields = json.loads(received)
            except (TimeoutError, OSError):
                received, address, fields = (None, None, None)
            
            if (fields is not None) and (fields['magic'] == sxglobals.magic):
                nodes = sxglobals.nodes
                nodes.append((fields['address'], fields['host'], fields['system'], fields['cores'], fields['status']))
                sxglobals.nodes = list(set(nodes))
                gui.table_grid(gui.tab3, gui.update_node_grid_data(), 5, 2)
            elif sxglobals.share_cpus and (fields is not None) and (fields['magic'] == sxglobals.magic_task):
                sxglobals.remote_assignment.append(fields)
                if len(sxglobals.remote_assignment) == int(fields['batch_size']):
                    print('Processing remotely assigned tasks')
                    manager.task_handler(remote_task=True)


# ------------------------------------------------------------------------
#    Network Node Task Listener
#    Receives files for processing
# ------------------------------------------------------------------------
class SXBATCHER_node_file_listener_thread(threading.Thread):
    def __init__(self, address, port):
        super().__init__()
        self.stop_event = threading.Event()
        self.address = address
        self.port = port
        self.bufsize = 4096
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('', self.port))


    def stop(self):
        self.stop_event.set()
        self.sock.close()


    def run(self):
        while not self.stop_event.is_set():
            try:
                # bind to given address and wait
                # self.sock.bind(self.address)
                self.sock.listen()
                """ accept() returns a new socket 
                for some unfathomable reason """
                conn, addr = self.sock.accept()
                print(f'[+] got connection {addr}')
                metadata = json.loads(conn.recv(self.bufsize).decode('utf-8'))

                # create batch submissions folder
                dir_name = 'batch_submissions'
                if not os.path.exists(dir_name):
                    os.mkdir(dir_name)
                    print("Folder" , dir_name,  "created")
                else:    
                    print("Folder" , dir_name,  "exists")

                target_dir = os.path.realpath('batch_submissions')

                metadata = {pathlib.Path(key).name:int(val) for key, val in metadata.items()}
                print(f'[+] got file list:')
                for fname in metadata:
                    print(f'\t{fname}')
                for file, size in metadata.items():
                    # with open(target_dir / file, 'wb') as f:
                    print('writing:', str(os.path.join(target_dir, file)))
                    with open(os.path.join(target_dir, file)) as f:
                        print(f'[+] writing into {file}...', end='')
                        # check remaining data size is more than zero
                        # read bufsize if there's more than bufsize left
                        # else modulo
                        left = size
                        while left:
                            quot, remain = divmod(left, self.bufsize)
                            left -= f.write(conn.recv(self.bufsize if quot else remain))
                            
                        print(f' {f.tell()}/{size}')
                conn.close()
                print('dead dead dead')

            except (OSError):
                print('File listener failure!')


# ------------------------------------------------------------------------
#    GUI
# ------------------------------------------------------------------------
class SXBATCHER_gui(object):
    def __init__(self):
        self.window = None
        self.tabs = None
        self.tab3 = None
        self.frame_a = None
        self.frame_b = None
        self.frame_c = None
        self.frame_items = None
        self.cat_var = None
        self.dropdown = None
        self.lb_items = None
        self.lb_export = None
        self.text_tags = None
        self.label_category = None
        self.label_item_count = None
        self.var_item_count = None
        self.var_export_count = None
        self.var_tag = None
        self.button_batch = None
        self.progress_bar = None
        self.label_progress = None
        self.table_nodes = None
        self.broadcast_thread = None
        self.discovery_thread = None
        self.file_receiving_thread = None
        return None


    def list_category(self, category, listbox):
        lb = listbox
        category = sxglobals.catalogue[category]
        for obj_dict in category.values():
            for obj_name in obj_dict['objects']:
                lb.insert('end', obj_name)
        return lb


    def handle_click_add_catalogue(self, event):
        self.lb_export.delete(0, 'end')
        for category in sxglobals.catalogue:
            self.lb_export = self.list_category(category, self.lb_export)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def handle_click_add_category(self, event):
        self.lb_export = self.list_category(sxglobals.category, self.lb_export)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def handle_click_add_selected(self, event):
        selected_item_list = [self.lb_items.get(i) for i in self.lb_items.curselection()]
        for value in selected_item_list:
            self.lb_export.insert('end', value)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def handle_click_add_tagged(self, event):
        tag = self.var_tag.get()
        for category in sxglobals.catalogue:
            for obj_dict in sxglobals.catalogue[category].values():
                if tag in obj_dict['tags']:
                    for obj_name in obj_dict['objects']:
                        self.lb_export.insert('end', obj_name)

        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def handle_click_update_plastic(self, event):
        if os.name == 'nt':
            subprocess.call(['C:\Program Files\PlasticSCM5\client\cm.exe', 'update', sxglobals.asset_path])
        else:
            subprocess.call(['/usr/local/bin/cm', 'update', sxglobals.asset_path])
        sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)


    def handle_click_listboxselect(self, event):
        tags = ''
        selected_item_list = [self.lb_items.get(i) for i in self.lb_items.curselection()]
        for obj in selected_item_list:
            for obj_dict in sxglobals.catalogue[sxglobals.category].values():
                if obj in obj_dict['objects']:
                    for tag in obj_dict['tags']:
                        tags += tag + ' '
            tags = tags + '\n'
        self.label_found_tags.configure(text='Tags in Selected:\n\n'+tags)


    def handle_click_start_batch(self, event):
        if (self.button_batch['state'] == 'normal') or (self.button_batch['state'] == 'active'):
            self.button_batch['state'] = 'disabled'

            manager.task_handler()


    def payload(self):
        return {
            "magic": sxglobals.magic,
            "address": sxglobals.ip_addr,
            "host": socket.gethostname(),
            "system": platform.system(),
            "cores": str(sxglobals.shared_cores),
            "status": "Busy" if sxglobals.node_busy_status else "Idle"
        }


    def handle_click_save_settings(self, event):
        init.save_conf()


    def step_check(self, t):
        self.window.after(1000, self.check_progress, t)


    def check_progress(self, t):
        if not t.is_alive():
            if len(sxglobals.errors) > 0:
                label_string = 'Job completed in '+str(round(sxglobals.now-sxglobals.then, 2))+' seconds\n'
                label_string += 'Errors in:\n'
                for file in sxglobals.errors:
                    label_string += file+'\n'
            else:
                label_string = 'Job completed in '+str(round(sxglobals.now-sxglobals.then, 2))+' seconds'
            manager.update_revisions()
            self.label_progress.configure(text=label_string)
            self.button_batch['state'] = 'normal'
            self.progress_bar['value'] = 0
            sxglobals.errors = []
        else:
            self.step_check(t)


    def refresh_lb_items(self):
        if self.lb_items is not None:
            self.lb_items.delete(0, 'end')
        else:
            self.lb_items = tk.Listbox(master=self.frame_items, selectmode='multiple')
        self.lb_items = self.list_category(sxglobals.category, self.lb_items)


    def clear_selection(self, event):
        self.lb_items.selection_clear(0, 'end')
        self.label_found_tags.configure(text='Tags in Selected:')


    def clear_lb_export(self, event):
        self.label_progress.configure(text='Idle')
        self.lb_export.delete(0, 'end')
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def toggle_batch_button(self):
        if (init.validate_paths() and (self.lb_export.size() > 0)):
            self.button_batch['state'] = 'normal'
        else:
            self.button_batch['state'] = 'disabled'


    def update_table_string(self):
        # table_string = '\nIP Address\tHost\tSystem\tCores\tStatus'
        table_string = ''
        for node in sxglobals.nodes:
            for item in node:
                table_string = table_string + str(item) + '\t'
            table_string = table_string + '\n'

        return table_string


    def update_node_grid_data(self):
        table_data = [['IP Address', 'Host Name', 'System', 'Cores', 'Status']]
        for node in sxglobals.nodes:
            node_row = []
            for item in node:
                node_row.append(item)
            table_data.append(node_row)

        return table_data


    def table_grid(self, root, data, startrow, startcolumn):
        rows = len(data)
        columns = len(data[0])
        for i in range(rows):
            for j in range(columns):
                self.e = tk.Entry(root)
                self.e.grid(row=i+startrow, column=j+startcolumn)
                self.e.insert('end', data[i][j])


    def draw_window(self):
        def display_selected(choice):
            sxglobals.category = self.cat_var.get()
            self.refresh_lb_items()
            self.label_item_count.configure(text='Items: '+str(self.lb_items.size()))


        def refresh_catalogue_view():
            sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)
            sxglobals.categories = list(sxglobals.catalogue.keys())
            sxglobals.category = sxglobals.categories[0]
            self.cat_var.set(sxglobals.category)
            self.dropdown['values'] = sxglobals.categories
            display_selected(self.cat_var.get())


        def update_blender_path(var, index, mode):
            sxglobals.blender_path = e1_str.get()
            self.toggle_batch_button()


        def update_sxtools_path(var, index, mode):
            sxglobals.sxtools_path = e2_str.get()
            self.toggle_batch_button()


        def update_catalogue_path(var, index, mode):
            sxglobals.catalogue_path = e3_str.get()
            refresh_catalogue_view()
            self.toggle_batch_button()


        def update_export_path(var, index, mode):
            sxglobals.export_path = e4_str.get()
            self.toggle_batch_button()


        def update_palette_override(var, index, mode):
            sxglobals.palette = c1_bool.get()
            sxglobals.palette_name = e5_str.get()


        def update_subdivision_override(var, index, mode):
            sxglobals.subdivision = c2_bool.get()

            try:
                subdivisions = e6_int.get()
            except Exception:
                subdivisions = 0

            if subdivisions < 0:
                sxglobals.subdivision_count = 0
            else:
                sxglobals.subdivision_count = subdivisions


        def update_flatten_override(var, index, mode):
            sxglobals.static_vertex_colors = c3_bool.get()


        def update_debug_override(var, index, mode):
            sxglobals.debug = c4_bool.get()


        def update_revision_export(var, index, mode):
            sxglobals.revision_export = c5_bool.get()


        def update_share_cpus(var, index, mode):
            sxglobals.share_cpus = core_count_bool.get()

            if sxglobals.share_cpus:
                self.file_receiving_thread = SXBATCHER_node_file_listener_thread(sxglobals.ip_addr, sxglobals.file_transfer_port)
                self.file_receiving_thread.start()
                if __debug__:
                    print('SX Batcher: File receiving started')
            else:
                self.file_receiving_thread.stop()
                if __debug__:
                    print('SX Batcher: File receiving stopped')

            if self.broadcast_thread is None:
                if sxglobals.share_cpus:
                    self.broadcast_thread = SXBATCHER_node_broadcast_thread(self.payload(), sxglobals.group, sxglobals.discovery_port)
                    self.broadcast_thread.start()
                    if __debug__:
                        print('SX Batcher: Node broadcasting started')
                else:
                    pass
            else:
                if sxglobals.share_cpus and not self.broadcast_thread.is_alive():
                    self.broadcast_thread = SXBATCHER_node_broadcast_thread(self.payload(), sxglobals.group, sxglobals.discovery_port)
                    self.broadcast_thread.start()
                    if __debug__:
                        print('SX Batcher: Node broadcasting restarted')
                elif not sxglobals.share_cpus and self.broadcast_thread.is_alive():
                    self.broadcast_thread.stop()
                    if __debug__:
                        print('SX Batcher: Node broadcasting stopped')                  

            try:
                cores = core_count_int.get()
            except Exception:
                cores = 0

            if cores < 0:
                sxglobals.shared_cores = 0
                core_count_int.set(0)
            elif cores > sxglobals.num_cores:
                sxglobals.shared_cores = sxglobals.num_cores
                core_count_int.set(sxglobals.num_cores)
            else:
                sxglobals.shared_cores = cores


        def update_use_nodes(var, index, mode):
            sxglobals.use_network_nodes = use_nodes_bool.get()
            if self.discovery_thread is None:
                if sxglobals.use_network_nodes:
                    self.discovery_thread = SXBATCHER_node_discovery_thread(sxglobals.group, sxglobals.discovery_port)
                    self.discovery_thread.start()
                    if __debug__:
                        print('SX Batcher: Node discovery started')
                else:
                    pass
            else:
                if sxglobals.use_network_nodes and not self.discovery_thread.is_alive():
                    self.discovery_thread = SXBATCHER_node_discovery_thread(sxglobals.group, sxglobals.discovery_port)
                    self.discovery_thread.start()
                    if __debug__:
                        print('SX Batcher: Node discovery restarted')
                elif not sxglobals.use_network_nodes and self.discovery_thread.is_alive():
                    self.discovery_thread.stop()
                    if __debug__:
                        print('SX Batcher: Node discovery stopped')       


        def browse_button_bp():
            e1_str.set(filedialog.askopenfilename())
            init.validate_paths()

        def browse_button_sp():
            e2_str.set(filedialog.askdirectory())
            init.validate_paths()


        def browse_button_cp():
            e3_str.set(filedialog.askopenfilename())
            init.validate_paths()


        def browse_button_ep():
            e4_str.set(filedialog.askdirectory())
            init.validate_paths()


        self.window = tk.Tk()
        self.window.title('SX Batcher')

        # Top tabs ------------------------------------------------------------
        self.tabs = ttk.Notebook(self.window)
        tab1 = ttk.Frame(self.tabs)
        tab2 = ttk.Frame(self.tabs)
        self.tab3 = ttk.Frame(self.tabs)

        self.tabs.add(tab1, text='Catalogue')
        self.tabs.add(tab2, text='Settings')
        self.tabs.add(self.tab3, text='Network')
        self.tabs.pack(expand = 1, fill ="both")

        # Content Tab ---------------------------------------------------------
        self.frame_a = tk.Frame(master=tab1, bd=10)
        self.frame_b = tk.Frame(master=tab1, bd=10)
        self.frame_c = tk.Frame(master=tab1, bd=10)
    
        # Frame A

        # Category OptionMenu
        self.cat_var = tk.StringVar()
        self.cat_var.set(sxglobals.category)

        # refresh_catalogue_view()
        self.dropdown = ttk.Combobox(self.frame_a, textvariable=self.cat_var)
        self.dropdown['values'] = sxglobals.categories
        self.dropdown['state'] = 'readonly'
        self.dropdown.pack(side='top', anchor='nw', expand=False)

        # source object list
        self.frame_items = tk.Frame(master=self.frame_a)
        self.refresh_lb_items()
        self.lb_items.pack(side='left', fill='both', expand=True)
        scrollbar_items = tk.Scrollbar(master=self.frame_items)
        scrollbar_items.pack(side='right', fill='y')
        self.lb_items.config(yscrollcommand=scrollbar_items.set)
        scrollbar_items.config(command=self.lb_items.yview)
        self.frame_items.pack(side='top', anchor='n', fill='both', expand=True)

        self.var_item_count = tk.IntVar()
        self.var_item_count.set(self.lb_items.size())
        self.label_item_count = tk.Label(master=self.frame_a, text='Items: '+str(self.var_item_count.get()))
        self.label_item_count.pack()

        button_clear_selection = tk.Button(
            master = self.frame_a,
            text="Clear Selection",
            width=20,
            height=3,
        )
        button_clear_selection.pack()


        # Frame B
        button_add_catalogue = tk.Button(
            master = self.frame_b,
            text="Add all from Catalogue",
            width=20,
            height=3,
        )
        button_add_catalogue.pack(pady=20)
        button_add_category = tk.Button(
            master = self.frame_b,
            text="Add all from Category",
            width=20,
            height=3,
        )
        button_add_category.pack()
        button_add_selected = tk.Button(
            master = self.frame_b,
            text="Add Selected",
            width=20,
            height=3,
        )
        button_add_selected.pack(pady=20)


        self.var_tag = tk.StringVar(self.window)
        tag_entry = tk.Entry(master=self.frame_b, textvariable=self.var_tag)
        tag_entry.pack()
        button_add_tagged = tk.Button(
            master = self.frame_b,
            text="Add Tagged",
            width=20,
            height=3,
        )
        button_add_tagged.pack(pady=10)


        button_clear_exports = tk.Button(
            master = self.frame_b,
            text="Clear Batch List",
            width=20,
            height=3,
        )
        button_clear_exports.pack(pady=30)

        self.label_found_tags = tk.Label(master=self.frame_b, text='Tags in Selected:')
        self.label_found_tags.pack()

        self.label_progress = tk.Label(master=self.frame_b, text='Idle')
        self.label_progress.pack(side='bottom')

        self.progress_bar = ttk.Progressbar(master=self.frame_b, orient='horizontal', length=100, mode='determinate')
        self.progress_bar.pack(side='bottom', anchor='s', pady=20)


        # Frame C
        self.label_exports = tk.Label(master=self.frame_c, text='Batch Objects:')
        self.label_exports.pack(side='top', anchor='nw')
        self.frame_export_items = tk.Frame(master=self.frame_c)
        self.lb_export = tk.Listbox(master=self.frame_export_items, selectmode='multiple')
        self.lb_export.pack(side='left', fill='both', expand=True)
        scrollbar_export_items = tk.Scrollbar(master=self.frame_export_items)
        scrollbar_export_items.pack(side='right', fill='y')
        self.lb_export.config(yscrollcommand=scrollbar_export_items.set)
        scrollbar_export_items.config(command=self.lb_export.yview)
        self.frame_export_items.pack(fill='both', expand=True)

        self.var_export_count = tk.IntVar()
        self.var_export_count.set(self.lb_export.size())
        self.label_export_item_count = tk.Label(master=self.frame_c, text='Items: '+str(self.var_export_count.get()))
        self.label_export_item_count.pack()

        self.button_batch = tk.Button(
            master = self.frame_c,
            text='Start Batch',
            width=20,
            height=3,
        )
        self.button_batch['state'] = 'disabled'
        self.button_batch.pack()

        self.frame_a.pack(side='left', fill='both', expand=True)
        self.frame_b.pack(side='left', fill='both', expand=True)
        self.frame_c.pack(side='left', fill='both', expand=True)

        # Event handling
        self.dropdown.bind('<<ComboboxSelected>>', display_selected)
        self.lb_items.bind('<<ListboxSelect>>', self.handle_click_listboxselect)
        button_add_catalogue.bind('<Button-1>', self.handle_click_add_catalogue)
        button_add_category.bind('<Button-1>', self.handle_click_add_category)
        button_add_selected.bind('<Button-1>', self.handle_click_add_selected)
        button_add_tagged.bind('<Button-1>', self.handle_click_add_tagged)
        button_clear_selection.bind('<Button-1>', self.clear_selection)
        button_clear_exports.bind('<Button-1>', self.clear_lb_export)
        self.button_batch.bind('<Button-1>', self.handle_click_start_batch)

        # Settings Tab --------------------------------------------------------
        l_title1 = tk.Label(tab2, text='Paths', justify='left', anchor='w')
        l_title1.grid(row=1, column=1, padx=10, pady=10)
        l1 = tk.Label(tab2, text='Blender Path:', width=20, justify='left', anchor='w')
        l1.grid(row=2, column=1, sticky='w', padx=10)
        l2 = tk.Label(tab2, text='SX Tools Library Path:', width=20, justify='left', anchor='w')
        l2.grid(row=3, column=1, sticky='w', padx=10)
        l3 = tk.Label(tab2, text='Catalogue Path:', width=20, justify='left', anchor='w')
        l3.grid(row=4, column=1, sticky='w', padx=10)
        l4 = tk.Label(tab2, text='Export Path:', width=20, justify='left', anchor='w')
        l4.grid(row=5, column=1, sticky='w', padx=10)

        e1_str=tk.StringVar(self.window)
        e2_str=tk.StringVar(self.window)
        e3_str=tk.StringVar(self.window)
        e4_str=tk.StringVar(self.window)

        e1_str.set(sxglobals.blender_path)
        e2_str.set(sxglobals.sxtools_path)
        e3_str.set(sxglobals.catalogue_path)
        e4_str.set(sxglobals.export_path)

        e1_str.trace_add('write', update_blender_path)
        e2_str.trace_add('write', update_sxtools_path)
        e3_str.trace_add('write', update_catalogue_path)
        e4_str.trace_add('write', update_export_path)

        e1 = tk.Entry(tab2, textvariable=e1_str, width=60)
        e1.grid(row=2, column=2)
        e2 = tk.Entry(tab2, textvariable=e2_str, width=60)
        e2.grid(row=3, column=2)
        e3 = tk.Entry(tab2, textvariable=e3_str, width=60)
        e3.grid(row=4, column=2)
        e4 = tk.Entry(tab2, textvariable=e4_str, width=60)
        e4.grid(row=5, column=2)

        l_empty = tk.Label(tab2, text=' ', width=10)
        l_empty.grid(row=1, column=3)

        button_browse_blenderpath = tk.Button(tab2, text='Browse', command=browse_button_bp)
        button_browse_blenderpath.grid(row=2, column=3)
        button_browse_sxtoolspath = tk.Button(tab2, text='Browse', command=browse_button_sp)
        button_browse_sxtoolspath.grid(row=3, column=3)
        button_browse_cataloguepath = tk.Button(tab2, text='Browse', command=browse_button_cp)
        button_browse_cataloguepath.grid(row=4, column=3)
        button_browse_exportpath = tk.Button(tab2, text='Browse', command=browse_button_ep)
        button_browse_exportpath.grid(row=5, column=3)

        button_save_settings = tk.Button(tab2, text='Save Settings')
        button_save_settings.grid(row=1, column=4, padx=10, pady=10)

        l_title2 = tk.Label(tab2, text='Overrides')
        l_title2.grid(row=6, column=1, padx=10, pady=10)

        c1_bool = tk.BooleanVar(self.window)
        c2_bool = tk.BooleanVar(self.window)
        c3_bool = tk.BooleanVar(self.window)
        c4_bool = tk.BooleanVar(self.window)
        c5_bool = tk.BooleanVar(self.window)
        e5_str=tk.StringVar(self.window)
        e6_int=tk.IntVar(self.window, value=0)

        c1_bool.set(sxglobals.palette)
        c2_bool.set(sxglobals.subdivision)
        c3_bool.set(sxglobals.static_vertex_colors)
        c4_bool.set(sxglobals.debug)
        c5_bool.set(sxglobals.revision_export)
        e5_str.set(sxglobals.palette_name)
        e6_int.set(sxglobals.subdivision_count)

        c1_bool.trace_add('write', update_palette_override)
        c2_bool.trace_add('write', update_subdivision_override)
        c3_bool.trace_add('write', update_flatten_override)
        c4_bool.trace_add('write', update_debug_override)
        c5_bool.trace_add('write', update_revision_export)
        e5_str.trace_add('write', update_palette_override)
        e6_int.trace_add('write', update_subdivision_override)

        c1 = tk.Checkbutton(tab2, text='Palette:', variable=c1_bool, justify='left', anchor='w')
        c1.grid(row=7, column=1, sticky='w', padx=10)
        c2 = tk.Checkbutton(tab2, text='Subdivision:', variable=c2_bool, justify='left', anchor='w')
        c2.grid(row=8, column=1, sticky='w', padx=10)
        c3 = tk.Checkbutton(tab2, text='Flatten Vertex Colors', variable=c3_bool, justify='left', anchor='w')
        c3.grid(row=9, column=1, sticky='w', padx=10)
        c4 = tk.Checkbutton(tab2, text='Blender Debug Output', variable=c4_bool, justify='left', anchor='w')
        c4.grid(row=10, column=1, sticky='w', padx=10)
        c5 = tk.Checkbutton(tab2, text='Only Batch Changed Revisions', variable=c5_bool, justify='left', anchor='w')
        c5.grid(row=11, column=1, sticky='w', padx=10)

        e5 = tk.Entry(tab2, textvariable=e5_str, width=20, justify='left')
        e5.grid(row=7, column=2, sticky='w')
        e6 = tk.Entry(tab2, textvariable=e6_int, width=3, justify='left')
        e6.grid(row=8, column=2, sticky='w')

        # Event handling
        button_save_settings.bind('<Button-1>', self.handle_click_save_settings)

        # Network
        l_title_pad = tk.Label(self.tab3, text=' ')
        l_title_pad.grid(row=1, column=1, padx=10, pady=10)
        l_title3 = tk.Label(self.tab3, text='Distributed Processing')
        l_title3.grid(row=1, column=2, padx=10, pady=10)

        core_count_bool = tk.BooleanVar(self.window)
        use_nodes_bool = tk.BooleanVar(self.window)
        core_count_int=tk.IntVar(self.window)

        core_count_bool.set(sxglobals.share_cpus)
        core_count_int.set(sxglobals.shared_cores)
        use_nodes_bool.set(sxglobals.use_network_nodes)

        core_count_bool.trace_add('write', update_share_cpus)
        use_nodes_bool.trace_add('write', update_use_nodes)
        core_count_int.trace_add('write', update_share_cpus)

        c5 = tk.Checkbutton(self.tab3, text='Share CPU Cores ('+str(sxglobals.num_cores)+'):', variable=core_count_bool, justify='left', anchor='w')
        c5.grid(row=2, column=2, sticky='w')
        c6 = tk.Checkbutton(self.tab3, text='Use Network Nodes', variable=use_nodes_bool, justify='left', anchor='w')
        c6.grid(row=3, column=2, sticky='w')

        e7 = tk.Entry(self.tab3, textvariable=core_count_int, width=3, justify='left')
        e7.grid(row=2, column=3, sticky='w')

        l_title5 = tk.Label(self.tab3, text='Node Discovery')
        l_title5.grid(row=4, column=2, padx=10, pady=10)

        self.table_grid(self.tab3, self.update_node_grid_data(), 5, 2)

        self.window.mainloop()


# ------------------------------------------------------------------------
#    NOTE: The catalogue file should be located in the root
#          of your asset folder structure.
# ------------------------------------------------------------------------

sxglobals = SXBATCHER_globals()
init = SXBATCHER_init()
gui = SXBATCHER_gui()
manager = SXBATCHER_batch_manager()
batch_local = SXBATCHER_batch_local()
batch_nodes = SXBATCHER_batch_nodes()

if __name__ == '__main__':
    gui.draw_window()

    if gui.broadcast_thread is not None:
        gui.broadcast_thread.stop()
    if gui.discovery_thread is not None:
        gui.discovery_thread.stop()


# TODO:
# - check for magic before parsing other node fields
# - node selection (to use remote only)
# - file collection
# - file transfer for remote assets
# - tmp folder location for remotely received source assets
# - tmp folder location for remote task result files (master-specific?)
# - note revision changes in catalogue while running
