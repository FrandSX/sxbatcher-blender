import argparse
import subprocess
import multiprocessing
import time
import json
import socket
import shutil
from multiprocessing import Pool
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
nodename = 'Node '+ip_addr


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
    parser.add_argument('-a', '--all', action='store_true', help='Export the entire Catalogue')
    parser.add_argument('-r', '--remotetask', nargs='+', type=str, help='Process list of files (distributed mode)')
    parser.add_argument('-d', '--folder', help='Ignore the Catalogue, export all objects from a folder')
    parser.add_argument('-c', '--category', help='Export all objects in a category (Default, Paletted...')
    parser.add_argument('-f', '--filename', help='Export an object by filename')
    parser.add_argument('-t', '--tag', help='Export all tagged objects')
    parser.add_argument('-s', '--sxtools', default=sxtools_path, help='SX Tools folder')
    parser.add_argument('-sd', '--subdivision', type=str, help='SX Tools subdivision override')
    parser.add_argument('-sp', '--palette', type=str, help='SX Tools palette override')
    parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='SX Tools flatten layers to VertexColor0')
    parser.add_argument('-e', '--exportpath', default=export_path, help='Export path')
    parser.add_argument('-l', '--listonly', action='store_true', help='Do not export, only list objects that match the other arguments')
    parser.add_argument('-v', '--verbose', action='store_true', help='Display debug messages')
    parser.add_argument('-u', '--updaterepo', action='store_true', help='Update art asset repository to the latest version (PlasticSCM)')
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


def sx_process(process_args):
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

    if export_path is not None:
        batch_args.extend(["-x", export_path])

    if sxtools_path is not None:
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
        for line in lines:
            if debug:
                print(line)
            else:
                if 'Error' in line:
                    print(line)
    except subprocess.CalledProcessError as error:
        print('SX Batch Error:', source_file)


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
    task_list = args.remotetask
    folder = str(args.folder)
    category = str(args.category)
    filename = str(args.filename)
    tag = str(args.tag)
    if args.subdivision is not None:
        subdivision = str(args.subdivision)
    else:
        subdivision = None
    if args.palette is not None:
        palette = str(args.palette)
    else:
        palette = None
    staticvertexcolors = args.staticvertexcolors
    debug = args.verbose
    catalogue_path = str(args.open)
    if args.open is not None:
        asset_path = os.path.split(catalogue_path)[0].replace('//', os.path.sep)
        if args.updaterepo:
            if os.name == 'nt':
                subprocess.call(['C:\Program Files\PlasticSCM5\client\cm.exe', 'update', asset_path])
            else:
                subprocess.call(['/usr/local/bin/cm', 'update', asset_path])
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
    elif (args.open is None) and (args.folder is None) and (args.remotetask is None):
        print(nodename + ' Error: No Catalogue or folder specified')
    else:
        # Build source file list according to arguments
        if args.remotetask is not None:
            for task in task_list:
                file_path = task.replace('//', os.path.sep)
                source_files.append(os.path.join(asset_path, file_path))
        elif args.folder is not None:
            source_files = [str(folder + os.sep + f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        elif len(asset_dict) > 0:
            if args.all:
                for category in asset_dict.keys():
                    for key in asset_dict[category].keys():
                        file_path = key.replace('//', os.path.sep)
                        source_files.append(os.path.join(asset_path, file_path))
            else:
                if args.category is not None:
                    if category in asset_dict.keys():
                        for key in asset_dict[category].keys():
                            file_path = key.replace('//', os.path.sep)
                            source_files.append(os.path.join(asset_path, file_path))
                if args.filename is not None:
                    for category in asset_dict.keys():
                        for key in asset_dict[category].keys():
                            if filename in key:
                                file_path = key.replace('//', os.path.sep)
                                source_files.append(os.path.join(asset_path, file_path))
                if args.tag is not None:
                    for category in asset_dict.keys():
                        for key, values in asset_dict[category].items():
                            for value in values:
                                if tag == value:
                                    file_path = key.replace('//', os.path.sep)
                                    source_files.append(os.path.join(asset_path, file_path))
                if (args.category is None) and (args.filename is None) and (args.tag is None):
                    print(nodename + ': Nothing selected for export')
        else:
            print(nodename + ' Error: Invalid Catalogue')

        # Construct node-specific task assignment list
        if len(source_files) > 0:
            source_files = list(set(source_files))
            print('\n' + nodename + ': Source files:')
            for file in source_files:
                print(file)

        # Generate task definition for each headless Blender
        tasks = []
        for file in source_files:
            tasks.append((blender_path, file, script_path, export_path, sxtools_path, subdivision, palette, staticvertexcolors, debug))

        if not args.listonly and (len(source_files) != 0):
            num_cores = multiprocessing.cpu_count()

            then = time.time()
            print(nodename + ': Spawning workers')

            with Pool(processes=num_cores, maxtasksperchild=1) as pool:
                for i, _ in enumerate(pool.imap(sx_process, tasks)):
                    print(nodename + ': Progress {0}%'.format(round(i/len(tasks)*100)))

            now = time.time()
            print(nodename + ':', len(source_files), 'files exported in', round(now-then, 2), 'seconds\n')

