import logging
import signal
import argparse
import threading
import subprocess
import multiprocessing
import time
import json
import socket
import pathlib
import struct
import os
import sys
import platform
import tkinter as tk
from tkinter import ttk, filedialog


# ------------------------------------------------------------------------
#    Globals
# ------------------------------------------------------------------------
class SXBATCHER_globals(object):
    def __init__(self):
        conf = init.load_conf()

        # Main mode, selected at launch, not saved in conf
        self.headless = False

        # Main locations, validate if changed
        self.blender_path = conf.get('blender_path', '')
        self.catalogue_path = conf.get('catalogue_path', '')
        self.export_path = conf.get('export_path', '')
        self.sxtools_path = conf.get('sxtools_path', '')

        # Distributed processing settings
        self.share_cpus = bool(int(conf.get('share_cpus', False)))
        self.shared_cores = int(conf.get('shared_cores', 0))
        self.use_network_nodes = bool(int(conf.get('use_nodes', False)))
        self.ip_addr = init.get_ip()
        self.performance_index = float(conf.get('performance_index', 0))

        self.then = None
        self.now = None
        self.remote_task = False

        # Batch lists
        self.export_objs = None
        self.source_files = None
        self.source_costs = None
        self.remote_assignment = []
        self.errors = []
        self.revision_dict = {}

        # Blender setting overrides
        self.debug = bool(int(conf.get('debug', False)))
        self.palette = bool(int(conf.get('palette', False)))
        self.palette_name = conf.get('palette_name', '')
        self.subdivision = bool(int(conf.get('subdivision', False)))
        self.subdivision_count = int(conf.get('subdivision_count', 0))
        self.static_vertex_colors = bool(int(conf.get('static_vertex_colors', False)))
        self.revision_export = bool(int(conf.get('revision_export', False)))

        # Network settings
        self.group = '239.1.1.1'
        self.discovery_port = 50000
        self.file_transfer_port = 50001
        self.magic = 'fna349fn'
        self.magic_task = 'snaf68yh'
        self.magic_result = 'ankdf89d'
        self.master_node = None
        self.buffer_size = 4096
        self.nodes = []
        self.tasked_nodes = []
        self.node_busy_status = False

        self.validate_paths()

        if self.catalogue_path != '':
            self.asset_path = os.path.split(self.catalogue_path)[0].replace('//', os.path.sep)
            self.catalogue = init.load_asset_data(self.catalogue_path)
        else:
            self.asset_path = ''
            self.catalogue = {'empty': {'empty': {'objects': ['empty', ], 'tags': ['empty', ]}}}

        self.active_category = list(self.catalogue.keys())[0]

        return None

    def validate_paths(self):
        if self.blender_path != '':
            self.blender_path = self.blender_path.replace('//', os.path.sep) if os.path.isfile(self.blender_path.replace('//', os.path.sep)) else ''
        if self.catalogue_path != '':
            self.catalogue_path = self.catalogue_path.replace('//', os.path.sep) if os.path.isfile(self.catalogue_path.replace('//', os.path.sep)) else ''
        if self.export_path != '':
            self.export_path = self.export_path.replace('//', os.path.sep) if os.path.isdir(self.export_path.replace('//', os.path.sep)) else ''
        if self.sxtools_path != '':
            self.sxtools_path = self.sxtools_path.replace('//', os.path.sep) if os.path.isdir(self.sxtools_path.replace('//', os.path.sep)) else ''

        return (self.blender_path != '') and (self.catalogue_path != '') and (self.export_path != '') and (self.sxtools_path != '')


# ------------------------------------------------------------------------
#    Exit Handler
# ------------------------------------------------------------------------
class SXBATCHER_exit_handler:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True


# ------------------------------------------------------------------------
#    Initialization and I/O
# ------------------------------------------------------------------------
class SXBATCHER_init(object):
    def __init__(self):
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


    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-ng', '--nogui', action='store_true', help='Run in headless mode')
        parser.add_argument('-nw', '--node', action='store_true', help='Run as headless worker node')
        parser.add_argument('-b', '--blenderpath', help='Blender executable location')
        parser.add_argument('-o', '--open', help='Open a Catalogue file')
        parser.add_argument('-s', '--sxtools', help='SX Tools folder')
        parser.add_argument('-e', '--exportpath', help='Export path')
        parser.add_argument('-a', '--all', action='store_true', help='Export the entire Catalogue')
        parser.add_argument('-c', '--category', help='Export all objects in a category (Default, Paletted...')
        parser.add_argument('-t', '--tag', help='Export all tagged objects')
        parser.add_argument('-d', '--folder', help='Ignore the Catalogue, export all objects from a folder')
        parser.add_argument('-sd', '--subdivision', type=str, help='SX Tools subdivision override')
        parser.add_argument('-sp', '--palette', type=str, help='SX Tools palette override')
        parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='SX Tools flatten layers to VertexColor0')
        parser.add_argument('-v', '--verbose', action='store_true', help='Display Blender debug messages')
        parser.add_argument('-re', '--revisionexport', action='store_true', help='Export changed revisions ')
        parser.add_argument('-cpu', '--sharecpus', help='Select number of logical cores for node')
        parser.add_argument('-un', '--usenodes', action='store_true', help='Use network nodes for distributed processing')
        parser.add_argument('-dn', '--detectnodes', help='Detect worker nodes in the network')
        parser.add_argument('-l', '--logfile', help='Logfile name')
        parser.add_argument('-ll', '--loglevel', type=str.lower, help="Standard loglevels", choices=['debug', 'info', 'warning', 'error', 'critical'], default='info')
        all_arguments, ignored = parser.parse_known_args()

        return all_arguments


    def update_globals(self, args):
        # Update path globals
        if args.blenderpath is not None:
            sxglobals.blender_path = os.path.abspath(args.blenderpath)
        else:
            if sxglobals.blender_path is None:
                logging.error('Blender path not specified')
        if args.open is not None:
            sxglobals.catalogue_path = os.path.abspath(args.open)
            sxglobals.asset_path = os.path.split(sxglobals.catalogue_path)[0].replace('//', os.path.sep)
        else:
            if sxglobals.catalogue_path is None:
                logging.error('Catalogue path not specified')
        if args.sxtools is not None:
            sxglobals.sxtools_path = os.path.abspath(args.sxtools)
        else:
            if sxglobals.sxtools_path is None:
                logging.error('SX Tools path not specified')
        if args.exportpath is not None:
            sxglobals.export_path = os.path.abspath(args.exportpath)
        else:
            if sxglobals.export_path is None:
                logging.error('Export collection path not specified')
        # TODO: Validate paths!

        # Update overrides
        if args.subdivision is not None:
            sxglobals.subdivision = True
            sxglobals.subdivision_count = int(args.subdivision)
        if args.palette is not None:
            sxglobals.palette = str(args.palette)
        if args.staticvertexcolors:
            sxglobals.staticvertexcolors = args.staticvertexcolors

        # Update batch processing options
        if args.revisionexport:
            sxglobals.revision_export = True
        else:
            sxglobals.revision_export = False
        if args.sharecpus is not None:
            sxglobals.share_cpus = True
            sxglobals.shared_cores = max(0, min(int(args.sharecpus), multiprocessing.cpu_count()))
        else:
            sxglobals.share_cpus = False
        if args.usenodes:
            sxglobals.use_network_nodes = True
        else:
            sxglobals.use_network_nodes = False

        # Populate export objects
        if args.all:
            sxglobals.export_objs = manager.get_catalogue_objs()
        elif args.category is not None:
            sxglobals.export_objs = manager.get_category_objs(str(args.category))
        elif args.tag is not None:
            sxglobals.export_objs = manager.get_tagged_objs([str(args.tag), ])

        # Determine headless or gui
        if args.nogui or args.node or sxglobals.export_objs is not None:
            sxglobals.headless = True


    def payload(self):
        return {
            "magic": sxglobals.magic,
            "address": sxglobals.ip_addr,
            "host": socket.gethostname(),
            "system": platform.system(),
            "cores": str(sxglobals.shared_cores),
            "performance_index": str(sxglobals.performance_index),
            "status": "Busy" if sxglobals.node_busy_status else "Idle"
        }


    def load_json(self, file_path):
        try:
            with open(file_path, 'r') as input:
                temp_dict = {}
                temp_dict = json.load(input)
                input.close()
            return temp_dict
        except ValueError:
            logging.error(f'Node {sxglobals.ip_addr} Error: Invalid JSON file {file_path}')
            return {}
        except IOError:
            logging.error(f'Node {sxglobals.ip_addr} Error: Failed to open JSON file {file_path}')
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
        temp_dict = {
            'blender_path': sxglobals.blender_path.replace(os.path.sep, '//') if sxglobals.blender_path != '' else '',
            'catalogue_path' : sxglobals.catalogue_path.replace(os.path.sep, '//') if sxglobals.catalogue_path != '' else '',
            'export_path': sxglobals.export_path.replace(os.path.sep, '//') if sxglobals.export_path != '' else '',
            'sxtools_path': sxglobals.sxtools_path.replace(os.path.sep, '//') if sxglobals.sxtools_path != '' else '',
            'debug': str(int(sxglobals.debug)),
            'palette': str(int(sxglobals.palette)),
            'palette_name': sxglobals.palette_name,
            'subdivision': str(int(sxglobals.subdivision)),
            'subdivision_count': str(sxglobals.subdivision_count),
            'static_vertex_colors': str(int(sxglobals.static_vertex_colors)),
            'revision_export': str(int(sxglobals.revision_export)),
            'share_cpus': str(int(sxglobals.share_cpus)),
            'shared_cores': str(int(sxglobals.shared_cores)),
            'use_nodes': str(int(sxglobals.use_network_nodes)),
            'performance_index': str(sxglobals.performance_index)
        }

        self.save_json(conf_path, temp_dict)
        logging.info(f'{conf_path} saved')


    def load_asset_data(self, catalogue_path):
        if os.path.isfile(catalogue_path):
            return self.load_json(catalogue_path)
        else:
            logging.error(f'Node {sxglobals.ip_addr} Error: Invalid Catalogue path')
            return {'empty': {'empty': {'objects': ['empty', ], 'tags': ['empty', ]}}}


    # revision data is expected to be located at the root of the export folder
    def load_revision_data(self):
        revision_path = sxglobals.export_path + os.path.sep + 'file_revisions.json'
        if os.path.isfile(revision_path):
            return self.load_json(revision_path)
        else:
            logging.error(f'Node {sxglobals.ip_addr} Error: file_revisions.json not found. A new one is created.')
            revision_dict = manager.get_revisions(all=True)
            self.save_json(revision_path, revision_dict)
            return revision_dict


    # files are path objects, address is a tuple of IP address and port
    def transfer_files(self, address, out_files):
        payload = out_files[0]
        files = out_files[1]
        sizemap = [(file.name, file.stat().st_size) for file in files]
        payload.insert(0, sizemap)
        bufsize = sxglobals.buffer_size

        try:
            with socket.create_connection(address, timeout=10) as sock:
                sock.sendall(json.dumps(payload).encode('utf-8'))
    
            with socket.create_connection(address, timeout=60) as sock:
                for file in files:
                    with open(file, 'rb') as f:
                        logging.debug(f'Transferring {file}')
                        while chunk := f.read(bufsize):
                            sock.send(chunk)
            return True
        except (ConnectionResetError, TimeoutError):
            return False


# ------------------------------------------------------------------------
#    Batch Manager for Localhost and Nodes
# ------------------------------------------------------------------------
class SXBATCHER_batch_manager(object):
    def __init__(self):
        return None


    def get_catalogue_objs(self):
        obj_list = []
        for category in sxglobals.catalogue:
            obj_list.extend(self.get_category_objs(category))
        return obj_list


    def get_category_objs(self, category):
        obj_list = []
        if category in sxglobals.catalogue:
            category = sxglobals.catalogue[category]
            for obj_dict in category.values():
                for obj_name in obj_dict['objects']:
                    obj_list.append(obj_name)
        else:
            logging.error('No matching category found.')
            logging.error(f'Existing categories are: {list(sxglobals.catalogue.keys())}')
        return obj_list


    def get_tagged_objs(self, tags):
        obj_list = []
        for category in sxglobals.catalogue:
            for obj_dict in sxglobals.catalogue[category].values():
                if any(tag in tags for tag in obj_dict['tags']):
                    obj_list.extend(obj_dict['objects'])
        return obj_list


    def remove_inactive_nodes(self):
        nodes = []
        for node in sxglobals.nodes:
            if int(time.time()) - node[5] < 15:
                nodes.append(node)
        sxglobals.nodes = nodes[:]


    def get_source_assets(self, revisions_only=False, costs=False):
        current_revisions = init.load_revision_data()
        new_revisions = {}

        # Also update catalogue in case of new revisions
        sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)
        sxglobals.active_category = sxglobals.active_category if sxglobals.active_category in sxglobals.catalogue else list(sxglobals.catalogue.keys())[0]

        source_assets = []
        changed_assets = []
        for obj in sxglobals.export_objs:
            for category in sxglobals.catalogue:
                for asset, obj_dict in sxglobals.catalogue[category].items():
                    if obj in obj_dict['objects']:
                        if revisions_only:
                            revision = obj_dict.get('revision', str(0))
                            if int(current_revisions.get(asset, '-1')) < int(revision):
                                new_revisions[asset] = revision
                                source_assets.append((asset, int(obj_dict['cost'])))
                                changed_assets.append(asset)
                        else:
                            source_assets.append((asset, int(obj_dict['cost'])))

        if revisions_only and len(changed_assets) > 0:
            changed_assets = list(set(changed_assets))
            logging.info(f'Revision changed in {changed_assets}')
        elif revisions_only and len(changed_assets) == 0:
            logging.info('No revision changes in selected assets')

        source_assets = list(set(source_assets))

        # current_revisions.update(new_revisions)
        # init.save_json(revision_path, current_revisions)

        source_assets.sort(key=lambda x: x[1], reverse=True)
        source_files = []
        for asset in source_assets:
            source_files.append(asset[0])

        if costs:
            return source_assets
        else:
            return source_files


    def get_revisions(self, all=False):
        source_assets = []
        for category in sxglobals.catalogue:
            for filepath, obj_dict in sxglobals.catalogue[category].items():
                if all:
                    source_assets.append((filepath, obj_dict.get('revision', str(0))))
                else:
                    if any(item in sxglobals.export_objs for item in obj_dict['objects']):
                        source_assets.append((filepath, obj_dict.get('revision', str(0))))

        source_assets = list(set(source_assets))
        revision_dict = {}
        for asset in source_assets:
            revision_dict[asset[0]] = asset[1]
        return revision_dict


    def update_revisions(self):
        revision_path = sxglobals.export_path + os.path.sep + 'file_revisions.json'
        file_dict = init.load_revision_data()
        data_dict = self.get_revisions()
        file_dict.update(data_dict)
        init.save_json(revision_path, file_dict)


    def delete_submissions(self):
        deleted = False
        with os.scandir(os.path.realpath('batch_submissions')) as submissions:
            for file in submissions:
                if file.name.endswith('.blend') and file.is_file():
                    deleted = True
                    os.remove(file)
        if deleted:
            logging.debug('Cleaned batch_submissions folder')


    def finish_task(self, reset=False):
        if reset:
            if sxglobals.revision_export:
                label_string = 'No revision changes'
            else:
                label_string = 'Could not start batch, check node settings'
        else:
            if sxglobals.master_node is None:
                self.update_revisions()

            if len(sxglobals.errors) > 0:
                label_string = 'Job completed in '+str(round(sxglobals.now-sxglobals.then, 2))+' seconds\n'
                logging.error(f'Node {sxglobals.ip_addr}: {label_string}')
                label_string += 'Errors in:\n'
                for file in sxglobals.errors:
                    label_string += file+'\n'
                    logging.error(f'Node {sxglobals.ip_addr}: {file}')
            else:
                label_string = 'Job completed in '+str(round(sxglobals.now-sxglobals.then, 2))+' seconds'
                logging.info(f'Node {sxglobals.ip_addr}: {label_string}')

        if not sxglobals.headless:
            gui.state_manager('ready', label=label_string)
        sxglobals.errors = []
        sxglobals.node_busy_status = False
        sxglobals.master_node = None
        sxglobals.remote_assignment = []

        if sxglobals.share_cpus:
            self.delete_submissions()


    def benchmark(self):
        benchmark_task = (
            sxglobals.blender_path,
            'perf_test.blend',
            str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py'),
            str(os.path.realpath('batch_results')),
            os.path.abspath(sxglobals.sxtools_path),
            None,
            None,
            False,
            False
            )
        try:
            os.makedirs(os.path.realpath('batch_results'), exist_ok=True)
            then = time.perf_counter()
            t = threading.Thread(target=batch_local.worker_process, args=[benchmark_task, ])
            t.start()
            t.join()
            now = time.perf_counter()
            logging.info(f'Node {sxglobals.ip_addr} benchmark result {now-then: .2f} seconds')
            sxglobals.performance_index = round(now-then, 2)
            os.remove(str(os.path.join(os.path.realpath('batch_results'), 'paletted/Suzanne_root.fbx')))
        except OSError:
            sxglobals.performance_index = 0


    # Handles task assignments:
    # 1) Local-only batch processing assigned via GUI
    # 2) Distributed batch processing assigned via GUI
    # 3) Work batches assigned by a remote node
    def task_handler(self, remote_task=False):
        def process_batch(tasks, num_cores):
            if sxglobals.headless:
                batch_local.worker_spawner(tasks, num_cores)
                self.finish_task()
            else:
                t = threading.Thread(target=batch_local.worker_spawner, args=(tasks, num_cores))
                t.start()
                gui.check_progress(t)


        sxglobals.node_busy_status = True
        sxglobals.then = time.perf_counter()

        if remote_task:
            # Receive files to be processed from network node
            if sxglobals.share_cpus and (len(sxglobals.remote_assignment) > 0):
                process_batch(self.prepare_received_tasks(), sxglobals.shared_cores)
            else:
                self.finish_task(reset=True)

        elif sxglobals.use_network_nodes:
            # Send files to be processed to network nodes
            if len(sxglobals.export_objs) > 0:
                node_tasks = self.prepare_node_tasks()
                logging.info(f'Node workload distribution { {node: len(tasks) for node, tasks in node_tasks.items()} }')
                # Track tasked nodes, check completions in file_listener_thread
                sxglobals.tasked_nodes = list(node_tasks.keys())
                for node_ip, task_list in node_tasks.items():
                    # Submit files to node
                    source_files = []
                    for task in task_list:
                        file_path = task['asset']
                        file_path.replace('//', os.path.sep)
                        source_path = pathlib.Path(os.path.join(sxglobals.asset_path, file_path))
                        source_files.append(source_path)

                    payload = []
                    for task in task_list:
                        task['asset'] = os.path.basename(task['asset'])
                        task['batch_size'] = str(len(task_list))
                        payload.append(task)

                    if len(source_files) > 0:
                        if init.transfer_files((node_ip, sxglobals.file_transfer_port), (payload, source_files)):
                            logging.info(f'{len(source_files)} source files transferred to Node {node_ip}')
                        else:
                            self.finish_task(reset=True)
            else:
                self.finish_task(reset=True)

        else:
            # Receive export list created in the UI
            if len(sxglobals.export_objs) > 0:
                logging.info(f'Processing {len(sxglobals.export_objs)} export objects')
                process_batch(self.prepare_local_tasks(), multiprocessing.cpu_count())
            else:
                self.finish_task(reset=True)


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
            logging.debug(f'\nNode {sxglobals.ip_addr} source files: {source_files}')

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
            if remote_task['subdivision'] == 'True':
                subdivision_count = str(remote_task['subdivision_count'])
            if remote_task['palette'] == 'True':
                palette_name = remote_task['palette_name']
            static_vertex_colors = True if remote_task['static_vertex_colors'] == 'True' else False
            debug = True if remote_task['debug'] == 'True' else False
            export_path = str(os.path.realpath('batch_results'))
            file = os.path.join(asset_path, asset)
 
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
        source_assets = self.get_source_assets(sxglobals.revision_export, costs=True)
        tasks = []
        node_tasks = {}

        if len(source_assets) > 0:
            for asset in source_assets:
                tasks.append({
                    "magic": sxglobals.magic_task,
                    "master": sxglobals.ip_addr,
                    "asset": asset[0],
                    "subdivision": str(sxglobals.subdivision),
                    "subdivision_count": str(sxglobals.subdivision_count),
                    "palette": str(sxglobals.palette),
                    "palette_name": sxglobals.palette_name,
                    "static_vertex_colors": str(sxglobals.static_vertex_colors),
                    "debug": str(sxglobals.debug),
                    "batch_size": str(len(source_assets)),
                    "cost": asset[1]
                })

            logging.debug(f'Source asset count: {len(tasks)}')

            # Sort nodes by performance rating
            nodes = sxglobals.nodes[:]
            if len(nodes) == 0:
                return []

            nodes.sort(key=lambda x: x[6])

            for node in nodes:
                node_ip = node[0]
                node_tasks[node_ip] = []

            # if any node has failed the benchmark, fall back to method 2
            method = 3
            for node in nodes:
                if not node[6] or float(node[6]) == 0:
                    logging.debug(f'Fallback method 2')
                    method = 2

            if method == 1:
                # Naive method: Divide tasks per node according to core counts
                workload = len(tasks)
                while workload > 0:
                    for node in nodes:
                        node_ip = node[0]
                        cores = int(node[3])
                        for i in range(cores):
                            if workload > i:
                                node_tasks[node_ip].append(tasks[len(tasks) - workload])
                                workload -= 1

            elif method == 2:
                # Cost based method: Divide tasks per node
                total_cores = 0
                for node in nodes:
                    total_cores += int(node[3])

                total_cost = 0
                for asset in source_assets:
                    total_cost += asset[1]
                logging.debug(f'Total cost: {total_cost}')

                # Allocate work share
                start = 0
                for node in nodes:
                    node_ip = node[0]
                    num_cores = int(node[3])
                    cost_share = total_cost * (float(num_cores) / float(total_cores))
                    logging.debug(f'Cost share: {node_ip} {cost_share}')

                    for i in range(start, len(tasks)):
                        if cost_share > 0:
                            node_tasks[node_ip].append(tasks[i])
                            cost_share -= tasks[i]['cost']
                            start += 1

            elif method == 3:
                # Cost-and-bias based method: Divide tasks per node
                perfs = []
                for node in nodes:
                    perfs.append(float(node[6]))
                best_perf = min(perfs)

                bias_cores = []
                for node in nodes:
                    bias_cores.append(round(float(node[3]) * best_perf / float(node[6])))
                    
                total_cores = sum(bias_cores)
                total_cost = 0
                for asset in source_assets:
                    total_cost += asset[1]
                logging.debug(f'Total cost: {total_cost}')

                cost_shares = []
                for num_cores in bias_cores:
                    cost_shares.append(total_cost * float(num_cores) / float(total_cores))

                # Allocate work share
                start = 0
                for i, node in enumerate(nodes):
                    node_ip = node[0]
                    logging.debug(f'Cost share: {node_ip} {cost_shares[i]}')

                    for j in range(start, len(tasks)):
                        if cost_shares[i] > 0:
                            node_tasks[node_ip].append(tasks[j])
                            cost_shares[i] -= tasks[j]['cost']
                            start += 1

        # remove empty task lists
        empty_nodes = []
        for node, tasks in node_tasks.items():
            if len(tasks) == 0:
                empty_nodes.append(node)
        if len(empty_nodes) > 0:
            for node in empty_nodes:
                del node_tasks[node]

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
            lines = p.stdout.splitlines()
            counter = 0
            for line in lines:
                if debug:
                    if 'clnors' not in line:
                        logging.debug(line)
                else:
                    if 'Error' in line:
                        counter = 10
                        logging.error(line)
                        return (source_file)
                    if counter > 0:
                        logging.error(line)
                        counter -= 1
        except subprocess.CalledProcessError as error:
            logging.critical(f'Blender process crashed - {source_file}')
            return (source_file)


    def worker_spawner(self, tasks, num_cores):
        logging.debug(f'Node {sxglobals.ip_addr} spawning workers')

        mp = multiprocessing.get_context("spawn")
        with mp.Pool(processes=num_cores, maxtasksperchild=1) as pool:
            for i, error in enumerate(pool.imap(self.worker_process, tasks)):
                progress = round((i + 1) / len(tasks) * 100)
                # logging.info(f'Node {sxglobals.ip_addr}: Progress {progress}%')
                if not sxglobals.headless:
                    gui.progress_bar['value'] = progress
                if error is not None:
                    # logging.error(error)
                    sxglobals.errors.append(error)
            pool.close()
            pool.join()

        sxglobals.now = time.perf_counter()
        export_count = len(sxglobals.remote_assignment) if len(sxglobals.remote_assignment) > 0 else len(sxglobals.export_objs) 
        logging.info(f'Node {sxglobals.ip_addr}: {export_count} objects exported in {sxglobals.now-sxglobals.then: .2f} seconds\n')
        if len(sxglobals.errors) > 0:
            logging.error(f'Node {sxglobals.ip_addr}: Errors in: {sxglobals.errors}')
    
        # transfer files to master node
        if sxglobals.share_cpus and len(sxglobals.remote_assignment) > 0:
            payload = []
            for_transfer = []
            for root, subdirs, files in os.walk('batch_results'):
                for file in files:
                    if file.endswith('.fbx'):
                        file_path = pathlib.Path(os.path.join(root, file))
                        payload.append({'magic': 'ankdf89d', file: os.path.basename(root)})
                        for_transfer.append(file_path)

            if len(payload) > 0:
                if init.transfer_files((sxglobals.master_node, sxglobals.file_transfer_port), (payload, for_transfer)):
                    for file in for_transfer:
                        os.remove(file)
                    logging.info(f'{len(for_transfer)} result files transferred to Node {sxglobals.master_node}, removed locally')
                else:
                    logging.critical('Failed to transfer result files')


# ------------------------------------------------------------------------
#    Network Node Broadcasting
#    Responsible for broadcasting availability of CPU resources
# ------------------------------------------------------------------------
class SXBATCHER_node_broadcast_thread(threading.Thread):
    def __init__(self, payload, group, port, timeout=10):
        super().__init__()
        # self.stop_event = threading.Event()
        self.group = group
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        self.sock.settimeout(timeout)


    # def stop(self):
    #     self.stop_event.set()
    #     self.sock.close()


    def run(self):
        while True:
            if sxglobals.share_cpus:
                try:
                    logging.debug(f'Broadcasting {json.dumps(init.payload())}')
                    self.payload = json.dumps(init.payload()).encode('utf-8')
                    self.sock.sendto(self.payload, (self.group, self.port))
                    time.sleep(3.0)
                except (TimeoutError, OSError):
                    logging.debug('Broadcast timeout, restarting')
            else:
                time.sleep(1.0)


# ------------------------------------------------------------------------
#    Network Node Discovery
#    Runs on host, receives multicast broadcasts from available nodes
# ------------------------------------------------------------------------
class SXBATCHER_node_discovery_thread(threading.Thread):
    def __init__(self, group, port, timeout=10):
        super().__init__()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.settimeout(timeout)
        self.sock.bind(('', port))
        packed = struct.pack('=4sl', socket.inet_aton(group), socket.INADDR_ANY)
        self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, packed)


    def run(self):
        while True:
            if sxglobals.use_network_nodes:
                try:
                    received, address = self.sock.recvfrom(sxglobals.buffer_size)
                    fields = json.loads(received)

                    # filter duplicates, maintain existing, update the status of the received node
                    if (fields['magic'] == sxglobals.magic) and (int(fields['cores']) > 0):
                        nodes = []
                        for node in sxglobals.nodes:
                            if node[0] != fields['address']:
                                nodes.append(node)
                        nodes.append((fields['address'], fields['host'], fields['system'], fields['cores'], fields['status'], int(time.time()), fields['performance_index']))
                        nodes.sort(key=lambda x: x[0])
                        sxglobals.nodes = nodes[:]
                    time.sleep(0.05)
                except (TimeoutError, OSError):
                    logging.debug('No nodes found for 10 seconds')
            else:
                time.sleep(1.0)


# ------------------------------------------------------------------------
#    Network Node File Listener
#    Receives files for and from processing
# ------------------------------------------------------------------------
class SXBATCHER_node_file_listener_thread(threading.Thread):
    def __init__(self, address, port):
        super().__init__()
        self.bufsize = sxglobals.buffer_size
        self.timeout = 90.0
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((address, port))
        self.sock.settimeout(self.timeout)


    def run(self):
        while True:
            if sxglobals.share_cpus or sxglobals.use_network_nodes:
                try:
                    self.sock.listen()
                    conn, addr = self.sock.accept()
                    logging.debug(f'Got connection {addr}')
                    os.makedirs(os.path.join(os.path.realpath('batch_results')), exist_ok=True)

                    # 1 - receive task data
                    b = bytearray()
                    while True:
                        chunk = conn.recv(self.bufsize)
                        if not chunk:
                            break
                        b.extend(chunk)
                    task_data = json.loads(b.decode('utf-8'))
                    logging.debug(f'Node {sxglobals.ip_addr}: Task data received')
                    conn.close()

                    file_meta = task_data.pop(0)
                    transfer_data = [(pathlib.Path(file_and_size[0]).name, int(file_and_size[1])) for file_and_size in file_meta]

                    # 2 - receive files
                    conn, addr = self.sock.accept()
                    for i, (file, size) in enumerate(transfer_data):
                        if task_data[i]['magic'] == sxglobals.magic_task:
                            target_dir = os.path.realpath('batch_submissions')
                        else:
                            target_dir = os.path.join(sxglobals.export_path, task_data[i][file])
                        os.makedirs(target_dir, exist_ok=True)

                        with open(os.path.join(target_dir, file), 'wb') as f:
                            left = size
                            while left:
                                quot, remain = divmod(left, self.bufsize)
                                left -= f.write(conn.recv(self.bufsize if quot else remain))
                            logging.debug(f'Wrote {file} ({f.tell()}/{size})')
                    conn.close()
                    logging.info(f'Node {sxglobals.ip_addr}: {len(transfer_data)} files received from Node {addr[0]}')

                    # check which nodes have finished their tasks based on connection address
                    if (addr[0] in sxglobals.tasked_nodes) and (task_data[0]['magic'] != sxglobals.magic_task):
                        sxglobals.tasked_nodes.remove(addr[0])
                        logging.info(f'Node {addr[0]} completed tasks')
                        if len(sxglobals.tasked_nodes) == 0:
                            sxglobals.now = time.perf_counter()
                            logging.info(f'All nodes finished')
                            manager.finish_task()

                    if sxglobals.share_cpus and (task_data is not None) and (task_data[0]['magic'] == sxglobals.magic_task):
                        sxglobals.master_node = task_data[0]['master']
                        for task in task_data:
                            sxglobals.remote_assignment.append(task)
                            if len(sxglobals.remote_assignment) == int(task['batch_size']):
                                logging.info(f'Node {sxglobals.ip_addr}: Processing {len(sxglobals.remote_assignment)} remotely assigned source files')
                                if sxglobals.headless:
                                    sxglobals.remote_task = True
                                else:
                                    gui.remote_task_bool.set(True)
                except (OSError, TimeoutError) as error:
                    logging.debug(f'Node {sxglobals.ip_addr} {error}')
            else:
                time.sleep(1.0)


# ------------------------------------------------------------------------
#    GUI
# ------------------------------------------------------------------------
class SXBATCHER_gui(tk.Tk):
    def state_manager(self, state=None, label=None):
        if state is None:
            if not sxglobals.use_network_nodes and sxglobals.validate_paths() and (self.lb_export.size() > 0):
                state = 'ready'
            elif sxglobals.use_network_nodes and len(sxglobals.nodes) > 0 and sxglobals.validate_paths() and (self.lb_export.size() > 0):
                state = 'ready'
            else:
                state = 'not_ready'

        if state == 'ready':
            self.button_start_batch['state'] = 'normal'
            self.progress_bar['value'] = 0
            self.remote_task_bool.set(False)
            if label is None:
                label = 'Ready to Process'

        elif state == 'remote':
            self.button_start_batch['state'] = 'disabled'
            self.progress_bar['value'] = 0
            if label is None:
                self.label_progress.configure(text='Processing Remote Batch')
            manager.task_handler(remote_task=True)

        elif state == 'busy':
            self.button_start_batch['state'] = 'disabled'
            self.progress_bar['value'] = 0
            if label is None:
                self.label_progress.configure(text='Processing Batch')

            sxglobals.export_objs = []
            for i in range(self.lb_export.size()):
                sxglobals.export_objs.append(self.lb_export.get(i))

            manager.task_handler()

        elif state == 'not_ready':
            self.button_start_batch['state'] = 'disabled'
            if label is None:
                if sxglobals.use_network_nodes:
                    label = 'Waiting for Tasks or Nodes'
                else:
                    label = 'Waiting for Tasks'

        self.label_progress.configure(text=label)


    def list_objs(self, obj_list, listbox):
        for obj_name in obj_list:
            listbox.insert('end', obj_name)
        return listbox


    def handle_click_batch_catalogue(self, event):
        self.lb_export.delete(0, 'end')
        self.lb_export = self.list_objs(manager.get_catalogue_objs(), self.lb_export)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.state_manager()


    def handle_click_batch_category(self, event):
        self.lb_export = self.list_objs(manager.get_category_objs(sxglobals.active_category), self.lb_export)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.state_manager()


    def handle_click_batch_selected(self, event):
        selected_item_list = [self.lb_items.get(i) for i in self.lb_items.curselection()]
        for value in selected_item_list:
            self.lb_export.insert('end', value)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.state_manager()


    def handle_click_batch_tagged(self, event):
        tag = self.var_tag.get()
        self.lb_export = self.list_objs(manager.get_tagged_objs([tag, ]), self.lb_export)
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.state_manager()


    def handle_click_listboxselect(self, event):
        tags = []
        selected_item_list = [self.lb_items.get(i) for i in self.lb_items.curselection()]
        for obj in selected_item_list:
            for obj_dict in sxglobals.catalogue[sxglobals.active_category].values():
                if obj in obj_dict['objects']:
                    for tag in obj_dict['tags']:
                        tags.append(tag)

        # sort tags by frequency, remove duplicates
        tags = sorted(tags, key=tags.count, reverse=True)
        tags = list(dict.fromkeys(tags))

        tag_string = ''
        for i, tag in enumerate(tags):
            tag_string += tag + ' '
            j = i + 1
            if (j % 5 == 0):
                tag_string += '\n'
        tag_string += '\n'

        prefix = ''
        if len(tag_string) > 0:
            prefix = 'Tags in Selected:\n'
        self.label_found_tags.configure(text=prefix+tag_string)


    def handle_click_start_batch(self, event):
        if (self.button_start_batch['state'] == 'normal') or (self.button_start_batch['state'] == 'active'):
            self.state_manager('busy')


    def handle_click_save_settings(self, event):
        init.save_conf()


    def check_progress(self, t):
        if not t.is_alive():
            t.join()
            manager.finish_task()
        else:
            self.after(1000, self.check_progress, t)


    def refresh_lb_items(self):
        self.lb_items.delete(0, 'end')
        self.lb_items = self.list_objs(manager.get_category_objs(sxglobals.active_category), self.lb_items)


    def clear_selection(self, event):
        self.lb_items.selection_clear(0, 'end')
        self.label_found_tags.configure(text='')


    def clear_lb_export(self, event):
        self.lb_export.delete(0, 'end')
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.state_manager('not_ready')


    def update_node_grid_data(self, height):
        node_data = ('', '', '', '', '')
        if len(sxglobals.nodes) == 0:
            nodes = [node_data]
        else:
            nodes = []
            for node in sxglobals.nodes:
                nodes.append(node[0:5])
            if len(sxglobals.nodes) < height:
                for i in range(height - len(sxglobals.nodes)):
                    nodes.append(node_data)
        return nodes


    def table_grid(self, root, data, startrow, startcolumn):
        rows = len(data)
        columns = len(data[0])
        for i in range(rows):
            for j in range(columns):
                self.e = tk.Entry(root)
                self.e.grid(row=i+startrow, column=j+startcolumn)
                self.e.insert('end', data[i][j])


    def __init__(self):
        super().__init__()
        self.title('SX Batcher')
        self.node_cache = []
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
        self.button_start_batch = None
        self.progress_bar = None
        self.label_progress = None
        self.table_nodes = None
        self.remote_task_bool = None


        def update_remote_process(var, index, mode):
             if self.remote_task_bool.get():
                self.state_manager('remote')


        def display_selected(choice):
            sxglobals.active_category = self.cat_var.get()
            self.refresh_lb_items()
            self.label_item_count.configure(text='Items: '+str(self.lb_items.size()))


        def refresh_catalogue_view():
            sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)
            categories = list(sxglobals.catalogue.keys())
            sxglobals.active_category = categories[0]
            self.cat_var.set(sxglobals.active_category)
            self.dropdown['values'] = categories
            display_selected(self.cat_var.get())


        def update_path(var, index, mode):
            if var == 'blender_path_var':
                sxglobals.blender_path = e1_str.get()
            elif var == 'sxtools_path_var':
                sxglobals.sxtools_path = e2_str.get()
            elif var == 'catalogue_path_var':
                sxglobals.catalogue_path = e3_str.get()
                refresh_catalogue_view()
            elif var == 'export_path_var':
                sxglobals.export_path = e4_str.get()
            self.state_manager()


        def update_item(var, index, mode):
            if var == 'palette_bool' or var == 'palettename_str':
                sxglobals.palette = c1_bool.get()
                sxglobals.palette_name = e5_str.get()
            elif var == 'subdivision_bool' or var == 'subdivision_int':
                sxglobals.subdivision = c2_bool.get()
                try:
                    subdivisions = e6_int.get()
                except Exception:
                    subdivisions = 0

                if subdivisions < 0:
                    sxglobals.subdivision_count = 0
                else:
                    sxglobals.subdivision_count = subdivisions
            elif var == 'flatten_bool':
                sxglobals.static_vertex_colors = c3_bool.get()
            elif var == 'debug_bool':
                sxglobals.debug = c4_bool.get()
            elif var == 'revision_bool':
                sxglobals.revision_export = c5_bool.get()
            elif var == 'share_cpus_bool' or var == 'share_cpus_int':
                sxglobals.share_cpus = core_count_bool.get()
                cpu_count = multiprocessing.cpu_count()

                if sxglobals.share_cpus and sxglobals.performance_index == 0:
                    if sxglobals.validate_paths():
                        manager.benchmark()

                # remove localhost from nodelist if sharing is disabled
                if not sxglobals.share_cpus:
                    for i, node in enumerate(sxglobals.nodes):
                        if sxglobals.ip_addr in node:
                            sxglobals.nodes.pop(i)
                            break
                try:
                    cores = core_count_int.get()
                except Exception:
                    cores = cpu_count

                if cores < 0:
                    sxglobals.shared_cores = 0
                    core_count_int.set(0)
                elif cores > cpu_count:
                    sxglobals.shared_cores = cpu_count
                    core_count_int.set(cpu_count)
                else:
                    sxglobals.shared_cores = cores
            elif var == 'use_nodes_bool':
                sxglobals.use_network_nodes = use_nodes_bool.get()
                if not sxglobals.use_network_nodes:
                    sxglobals.nodes = []
            elif var == 'debug_level_var':
                debug_level = self.debug_var.get()
                debug_levels = {
                    'Debug': logging.DEBUG,
                    'Info': logging.INFO,
                    'Warning': logging.WARNING,
                    'Error': logging.ERROR,
                    'Critical': logging.CRITICAL
                }
                logging.getLogger().setLevel(debug_levels[debug_level])

        def browse_button_bp():
            e1_str.set(filedialog.askopenfilename())
            sxglobals.validate_paths()

        def browse_button_sp():
            e2_str.set(filedialog.askdirectory())
            sxglobals.validate_paths()


        def browse_button_cp():
            e3_str.set(filedialog.askopenfilename())
            sxglobals.validate_paths()


        def browse_button_ep():
            e4_str.set(filedialog.askdirectory())
            sxglobals.validate_paths()


        # place your timer-run refresh elements here
        def late_loop():
            # Filter out offline nodes
            manager.remove_inactive_nodes()

            # Draw grid of nodes (exclude their lifetime variables)
            node_state = []
            for node in sxglobals.nodes:
                node_state.append(node[0:5])

            if node_state != self.node_cache:
                self.table_grid(self.tab3, self.update_node_grid_data(len(self.node_cache)), 6, 2)
                if len(sxglobals.nodes) == 0:
                    self.state_manager('not_ready')
                self.node_cache = node_state

            self.after(1000, late_loop)


        # Top tabs ------------------------------------------------------------
        self.tabs = ttk.Notebook(self)
        tab1 = ttk.Frame(self.tabs)
        tab2 = ttk.Frame(self.tabs)
        self.tab3 = ttk.Frame(self.tabs)

        self.tabs.add(tab1, text='Catalogue')
        self.tabs.add(tab2, text='Settings')
        self.tabs.add(self.tab3, text='Network')
        self.tabs.pack(expand=1, fill="both")

        # Content Tab ---------------------------------------------------------
        self.frame_a = tk.Frame(master=tab1, bd=10)
        self.frame_b = tk.Frame(master=tab1, bd=10)
        self.frame_c = tk.Frame(master=tab1, bd=10)
    
        # Frame A

        # Category OptionMenu
        self.cat_var = tk.StringVar()
        self.cat_var.set(sxglobals.active_category)

        # refresh_catalogue_view()
        self.dropdown = ttk.Combobox(self.frame_a, textvariable=self.cat_var)
        self.dropdown['values'] = list(sxglobals.catalogue.keys())
        self.dropdown['state'] = 'readonly'
        self.dropdown.pack(side='top', anchor='nw', expand=False)

        # source object list
        self.frame_items = tk.Frame(master=self.frame_a)
        self.lb_items = tk.Listbox(master=self.frame_items, selectmode='multiple')
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
            master=self.frame_a,
            text="Clear Selection",
            width=20,
            height=3,
        )
        button_clear_selection.pack()


        # Frame B
        button_batch_catalogue = tk.Button(
            master=self.frame_b,
            text="Add all from Catalogue",
            width=20,
            height=3,
        )
        button_batch_catalogue.pack(pady=20)
        button_batch_category = tk.Button(
            master=self.frame_b,
            text="Add all from Category",
            width=20,
            height=3,
        )
        button_batch_category.pack()
        button_batch_selected = tk.Button(
            master=self.frame_b,
            text="Add Selected",
            width=20,
            height=3,
        )
        button_batch_selected.pack(pady=20)


        self.var_tag = tk.StringVar(self)
        tag_entry = tk.Entry(master=self.frame_b, textvariable=self.var_tag)
        tag_entry.pack()
        button_batch_tagged = tk.Button(
            master=self.frame_b,
            text="Add Tagged",
            width=20,
            height=3,
        )
        button_batch_tagged.pack(pady=10)


        button_clear_exports = tk.Button(
            master=self.frame_b,
            text="Clear Batch List",
            width=20,
            height=3,
        )
        button_clear_exports.pack(pady=30)

        self.label_found_tags = tk.Label(master=self.frame_b, text='')
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

        self.button_start_batch = tk.Button(
            master=self.frame_c,
            text='Start Batch',
            width=20,
            height=3,
        )
        self.button_start_batch['state'] = 'disabled'
        self.button_start_batch.pack()

        self.frame_a.pack(side='left', fill='both', expand=True)
        self.frame_b.pack(side='left', fill='both', expand=True)
        self.frame_c.pack(side='left', fill='both', expand=True)

        # Event handling
        self.dropdown.bind('<<ComboboxSelected>>', display_selected)
        self.lb_items.bind('<<ListboxSelect>>', self.handle_click_listboxselect)
        button_batch_catalogue.bind('<Button-1>', self.handle_click_batch_catalogue)
        button_batch_category.bind('<Button-1>', self.handle_click_batch_category)
        button_batch_selected.bind('<Button-1>', self.handle_click_batch_selected)
        button_batch_tagged.bind('<Button-1>', self.handle_click_batch_tagged)
        button_clear_selection.bind('<Button-1>', self.clear_selection)
        button_clear_exports.bind('<Button-1>', self.clear_lb_export)
        self.button_start_batch.bind('<Button-1>', self.handle_click_start_batch)

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

        e1_str = tk.StringVar(self, name='blender_path_var')
        e2_str = tk.StringVar(self, name='sxtools_path_var')
        e3_str = tk.StringVar(self, name='catalogue_path_var')
        e4_str = tk.StringVar(self, name='export_path_var')

        e1_str.set(sxglobals.blender_path)
        e2_str.set(sxglobals.sxtools_path)
        e3_str.set(sxglobals.catalogue_path)
        e4_str.set(sxglobals.export_path)

        e1_str.trace_add('write', update_path)
        e2_str.trace_add('write', update_path)
        e3_str.trace_add('write', update_path)
        e4_str.trace_add('write', update_path)

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

        c1_bool = tk.BooleanVar(self, name='palette_bool')
        c2_bool = tk.BooleanVar(self, name='subdivision_bool')
        c3_bool = tk.BooleanVar(self, name='flatten_bool')
        c4_bool = tk.BooleanVar(self, name='debug_bool')
        c5_bool = tk.BooleanVar(self, name='revision_bool')
        e5_str = tk.StringVar(self, name='palettename_str')
        e6_int = tk.IntVar(self, value=0, name='subdivision_int')

        c1_bool.set(sxglobals.palette)
        c2_bool.set(sxglobals.subdivision)
        c3_bool.set(sxglobals.static_vertex_colors)
        c4_bool.set(sxglobals.debug)
        c5_bool.set(sxglobals.revision_export)
        e5_str.set(sxglobals.palette_name)
        e6_int.set(sxglobals.subdivision_count)

        c1_bool.trace_add('write', update_item)
        c2_bool.trace_add('write', update_item)
        c3_bool.trace_add('write', update_item)
        c4_bool.trace_add('write', update_item)
        c5_bool.trace_add('write', update_item)
        e5_str.trace_add('write', update_item)
        e6_int.trace_add('write', update_item)

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

        # Debug Level
        l_title3 = tk.Label(tab2, text='Debug Level')
        l_title3.grid(row=13, column=1, padx=10, pady=10)

        self.debug_var = tk.StringVar(self, name='debug_level_var')
        # self.debug_var.set('Info')

        self.debug_dropdown = ttk.Combobox(tab2, textvariable=self.debug_var)
        self.debug_dropdown['values'] = ['Debug', 'Info', 'Warning', 'Error', 'Critical']
        self.debug_dropdown['state'] = 'readonly'
        self.debug_dropdown.grid(row=13, column=2, sticky='w')

        self.debug_var.trace_add('write', update_item)

        # Event handling
        button_save_settings.bind('<Button-1>', self.handle_click_save_settings)

        # Network Tab ---------------------------------------------------------
        l_title_pad = tk.Label(self.tab3, text=' ')
        l_title_pad.grid(row=1, column=1, padx=10, pady=10)
        l_title3 = tk.Label(self.tab3, text='Distributed Processing')
        l_title3.grid(row=1, column=2, padx=10, pady=10)

        core_count_bool = tk.BooleanVar(self, name='share_cpus_bool')
        use_nodes_bool = tk.BooleanVar(self, name='use_nodes_bool')
        core_count_int = tk.IntVar(self, value=sxglobals.shared_cores, name='share_cpus_int')

        core_count_bool.trace_add('write', update_item)
        use_nodes_bool.trace_add('write', update_item)
        core_count_int.trace_add('write', update_item)

        core_count_bool.set(sxglobals.share_cpus)
        core_count_int.set(sxglobals.shared_cores)
        use_nodes_bool.set(sxglobals.use_network_nodes)

        c5 = tk.Checkbutton(self.tab3, text='Share CPU Cores ('+str(multiprocessing.cpu_count())+'):', variable=core_count_bool, justify='left', anchor='w')
        c5.grid(row=2, column=2, sticky='w')
        c6 = tk.Checkbutton(self.tab3, text='Use Network Nodes', variable=use_nodes_bool, justify='left', anchor='w')
        c6.grid(row=3, column=2, sticky='w')

        e7 = tk.Entry(self.tab3, textvariable=core_count_int, width=3, justify='left')
        e7.grid(row=2, column=3, sticky='w')

        l_title5 = tk.Label(self.tab3, text='Node Discovery')
        l_title5.grid(row=4, column=2, padx=10, pady=10)

        self.remote_task_bool = tk.BooleanVar(self)
        self.remote_task_bool.set(False)
        self.remote_task_bool.trace_add('write', update_remote_process)

        self.table_grid(self.tab3, [['IP Address', 'Host Name', 'System', 'Cores', 'Status'], ], 5, 2)

        late_loop()


# ------------------------------------------------------------------------
#    NOTE: The catalogue file should be located in the root
#          of your asset folder structure.
# ------------------------------------------------------------------------

init = SXBATCHER_init()
exit_handler = SXBATCHER_exit_handler()
sxglobals = SXBATCHER_globals()
manager = SXBATCHER_batch_manager()
batch_local = SXBATCHER_batch_local()

if __name__ == '__main__':
    args = init.get_args()

    logging.basicConfig(**{ k:v for k,v in (
        ('encoding', 'utf-8'),
        ('format', '%(asctime)s SX Batcher %(levelname)s: %(message)s'),
        ('datefmt','%H:%M:%S'),
        ('filename', pathlib.Path(__file__).parent.resolve() / (pathlib.Path(args.logfile).stem + '.log') if args.logfile else None),
        ('level', getattr(logging, args.loglevel.upper()) if args.loglevel else None)
    ) if v })

    # Pre-loop tasks and file batches
    if len(sys.argv) == 1:
        sxglobals.headless = False
    else:
        init.update_globals(args)

    broadcast_thread = SXBATCHER_node_broadcast_thread(init.payload(), sxglobals.group, sxglobals.discovery_port)
    broadcast_thread.daemon = True
    broadcast_thread.start()

    discovery_thread = SXBATCHER_node_discovery_thread(sxglobals.group, sxglobals.discovery_port)
    discovery_thread.daemon = True
    discovery_thread.start()

    file_receiving_thread = SXBATCHER_node_file_listener_thread(sxglobals.ip_addr, sxglobals.file_transfer_port)
    file_receiving_thread.daemon = True
    file_receiving_thread.start()

    # Main function tree
    if sxglobals.headless:
        if args.node:
            # Started in headless worker node
            logging.info('Starting in headless mode')
            logging.info(f'Listening for network tasks on port {sxglobals.discovery_port}')
            sxglobals.share_cpus = True
            sxglobals.shared_cores = multiprocessing.cpu_count()

            if sxglobals.performance_index == 0:
                if sxglobals.validate_paths():
                    manager.benchmark()

            while not exit_handler.kill_now:
                if sxglobals.remote_task:
                    sxglobals.remote_task = False
                    manager.task_handler(remote_task=True)
                time.sleep(1.0)
        else:
            if (sxglobals.export_objs is not None) and (len(sxglobals.export_objs) > 0):
                # Started as a master in distributed batch mode
                if sxglobals.use_network_nodes:
                    logging.info('Discovering network nodes (10 seconds)')
                    time.sleep(10.0)
                    if len(sxglobals.nodes) > 0:
                        logging.info('Nodes found:')
                        for node in sxglobals.nodes:
                            logging.info(node)
                        manager.task_handler()
                        while (len(sxglobals.tasked_nodes) > 0) and not exit_handler.kill_now:
                            # Master node may also be using its own CPUs as a worker node
                            if sxglobals.remote_task:
                                sxglobals.remote_task = False
                                manager.task_handler(remote_task=True)
                            time.sleep(1.0)
                    else:
                        logging.info('No network nodes discovered')
                else:
                    # Local batch
                    manager.task_handler()
            else:
                logging.info('Nothing specified for export')
    else:
        global gui
        gui = SXBATCHER_gui()
        gui.debug_var.set(args.loglevel.capitalize())
        gui.mainloop()

    logging.info('Exited gracefully')
