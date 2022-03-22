import subprocess
import multiprocessing
import time
import random
import json
import socket
import shutil
from multiprocessing import Pool
import os
import tkinter as tk
from tkinter import MULTIPLE, ttk
import argparse


# ------------------------------------------------------------------------
#    Globals
# ------------------------------------------------------------------------
class SXBATCHER_globals(object):
    def __init__(self):
        self.blender_path = None
        self.catalogue_path = None
        self.export_path = None
        self.sxtools_path = None
        self.ip_addr = None
        self.nodename = None
        self.catalogue = None
        self.categories = None
        self.category = None
        self.debug = False
        self.export_objs = None


    def __del__(self):
        print('SX Batcher: Exiting sxglobals')


# ------------------------------------------------------------------------
#    Initialization and I/O
# ------------------------------------------------------------------------
class SXBATCHER_init(object):
    def __init__(self):
        return None


    def __del__(self):
        print('SX Batcher: Exiting init')


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
            print(sxglobals.nodename + ' Error: Invalid JSON file.')
            return {}
        except IOError:
            print(sxglobals.nodename + ' Error: File not found!')
            return {}


    def load_conf(self):
        if os.path.isfile(os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')):
            conf_path = os.path.realpath(__file__).replace(os.path.basename(__file__), 'sx_conf.json')

            conf = self.load_json(conf_path)
            sxglobals.blender_path = conf.get('blender_path')
            sxglobals.catalogue_path = conf.get('catalogue_path')
            sxglobals.export_path = conf.get('export_path')
            sxglobals.sxtools_path = conf.get('sxtools_path')

            self.validate_paths()


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
            return {}


class SXBATCHER_gui(object):
    def __init__(self):
        self.window = None
        self.tabs = None
        self.frame_a = None
        self.frame_b = None
        self.frame_c = None
        self.frame_items = None
        self.lb_items = None
        self.lb_export = None
        self.label_category = None
        self.label_item_count = None
        self.var_item_count = None
        self.var_export_count = None
        self.button_batch = None

        return None


    def __del__(self):
        print('SX Batcher: Exiting gui')


    def list_category(self, category, listbox):
        lb = listbox
        item_dict = sxglobals.catalogue[category]
        for tag_list in item_dict.values():
            for tag in tag_list:
                if tag.endswith('_root'):
                    lb.insert('end', tag)
        return lb


    def handle_click(self, event):
        sxglobals.category = random.choice(sxglobals.categories)
        self.refresh_lb_items()
        self.label_item_count.configure(text='Items: '+str(self.lb_items.size()))


    def handle_click_change_category(self, event, category):
        sxglobals.category = category
        self.refresh_lb_items()
        self.label_item_count.configure(text='Items: '+str(self.lb_items.size()))


    def handle_click_add_catalogue(self, event):
        self.lb_export.delete(0, 'end')
        for category in sxglobals.catalogue.keys():
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


    def handle_click_start_batch(self, event):
        sxglobals.export_objs = []
        for i in range(self.lb_export.size()):
            sxglobals.export_objs.append(self.lb_export.get(i))
        process.export_selected()

    def refresh_lb_items(self):
        if self.lb_items is not None:
            self.lb_items.delete(0, 'end')
        else:
            self.lb_items = tk.Listbox(master=self.frame_items, selectmode='multiple')
        self.lb_items = self.list_category(sxglobals.category, self.lb_items)


    def clear_lb_export(self, event):
        self.lb_export.delete(0, 'end')
        self.label_export_item_count.configure(text='Items: '+str(self.lb_export.size()))
        self.toggle_batch_button()


    def toggle_batch_button(self):
        if (init.validate_paths() and (self.lb_export.size() > 0)):
            self.button_batch['state'] = 'normal'
        else:
            self.button_batch['state'] = 'disabled'


    def draw_window(self):
        def display_selected(choice):
            choice = variable.get()
            sxglobals.category = choice
            self.refresh_lb_items()
            self.label_item_count.configure(text='Items: '+str(self.lb_items.size()))

        def update_blender_path(var, index, mode):
            sxglobals.blender_path = e1_str.get()
            self.toggle_batch_button()

        def update_catalogue_path(var, index, mode):
            sxglobals.catalogue_path = e2_str.get()
            self.toggle_batch_button()

        def update_export_path(var, index, mode):
            sxglobals.export_path = e3_str.get()
            self.toggle_batch_button()

        def update_sxtools_path(var, index, mode):
            sxglobals.sxtools_path = e4_str.get()
            self.toggle_batch_button()

        self.window = tk.Tk()
        self.window.title('SX Batcher')

        # Top menu bar --------------------------------------------------------
        # menubar = tk.Menu(self.window)

        # menu_file = tk.Menu(menubar, tearoff=0)

        # menubar.add_cascade(label="File", menu=menu_file)

        # menu_file.add_command(label="Open Catalogue", command=temp_test())
        # menu_file.add_command(label="Settings", command=temp_test())
        # menu_file.add_command(label="Quit", command=self.window.quit)  
 
        # self.window.config(menu=menubar)

        # Top tabs ------------------------------------------------------------
        self.tabs = ttk.Notebook(self.window)
        tab1 = ttk.Frame(self.tabs)
        tab2 = ttk.Frame(self.tabs)
        tab3 = ttk.Frame(self.tabs)

        self.tabs.add(tab1, text='Catalogue')
        self.tabs.add(tab2, text='Settings')
        self.tabs.add(tab3, text='Network')
        self.tabs.pack(expand = 1, fill ="both")

        # Content Tab ---------------------------------------------------------
        self.frame_a = tk.Frame(master=tab1, bd=10)
        self.frame_b = tk.Frame(master=tab1, bd=10)
        self.frame_c = tk.Frame(master=tab1, bd=10)
    
        # Frame A

        # Category OptionMenu
        variable = tk.StringVar()
        variable.set(sxglobals.category)

        dropdown = tk.OptionMenu(
            self.frame_a,
            variable,
            *sxglobals.categories,
            command=display_selected
            )
        dropdown.pack(side='top', anchor='nw', expand=False)

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
        label_ip = tk.Label(master=self.frame_b, text=sxglobals.ip_addr)
        label_ip.pack()
        entry = tk.Entry(master=self.frame_b)
        entry.pack()
        button_add_catalogue = tk.Button(
            master = self.frame_b,
            text="Add all from Catalogue",
            width=20,
            height=3,
        )
        button_add_catalogue.pack()
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
        button_add_selected.pack()
        button_clear_exports = tk.Button(
            master = self.frame_b,
            text="Clear Batch List",
            width=20,
            height=3,
        )
        button_clear_exports.pack()


        # Frame C
        self.label_exports = tk.Label(master=self.frame_c, text='Batch Files:')
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
            text="Start Batch",
            width=20,
            height=3,
        )
        self.button_batch['state'] = 'disabled'
        self.button_batch.pack()

        self.frame_a.pack(side='left', fill='both', expand=True)
        self.frame_b.pack(side='left', fill='both', expand=True)
        self.frame_c.pack(side='left', fill='both', expand=True)

        # sxglobals.category = entry.get()

        button_add_catalogue.bind("<Button-1>", self.handle_click_add_catalogue)
        button_add_category.bind("<Button-1>", self.handle_click_add_category)
        button_add_selected.bind("<Button-1>", self.handle_click_add_selected)
        button_clear_exports.bind("<Button-1>", self.clear_lb_export)
        self.button_batch.bind("<Button-1>", self.handle_click_start_batch)

        # Settings Tab --------------------------------------------------------
        l_title1 = tk.Label(tab2, text='Paths', justify='left', anchor='w')
        l_title1.grid(row=1, column=1, padx=10, pady=10)
        l1 = tk.Label(tab2, text='Blender Path:', width=20, justify='left', anchor='w')
        l1.grid(row=2, column=1, sticky='w', padx=10)
        l2 = tk.Label(tab2, text='Catalogue Path:', width=20, justify='left', anchor='w')
        l2.grid(row=3, column=1, sticky='w', padx=10)
        l3 = tk.Label(tab2, text='Export Path:', width=20, justify='left', anchor='w')
        l3.grid(row=4, column=1, sticky='w', padx=10)
        l4 = tk.Label(tab2, text='SX Tools Path:', width=20, justify='left', anchor='w')
        l4.grid(row=5, column=1, sticky='w', padx=10)

        e1_str=tk.StringVar(self.window)
        e2_str=tk.StringVar(self.window)
        e3_str=tk.StringVar(self.window)
        e4_str=tk.StringVar(self.window)
        e1_str.trace_add('write', update_blender_path)
        e2_str.trace_add('write', update_catalogue_path)
        e3_str.trace_add('write', update_export_path)
        e4_str.trace_add('write', update_sxtools_path)

        e1 = tk.Entry(tab2, textvariable=e1_str, width=60)
        e1.grid(row=2, column=2)
        e2 = tk.Entry(tab2, textvariable=e2_str, width=60)
        e2.grid(row=3, column=2)
        e3 = tk.Entry(tab2, textvariable=e3_str, width=60)
        e3.grid(row=4, column=2)
        e4 = tk.Entry(tab2, textvariable=e4_str, width=60)
        e4.grid(row=5, column=2)

        e1_str.set(sxglobals.blender_path)
        e2_str.set(sxglobals.catalogue_path)
        e3_str.set(sxglobals.export_path)
        e4_str.set(sxglobals.sxtools_path)

        l_empty = tk.Label(tab2, text=' ', width=10)
        l_empty.grid(row=1, column=3)

        b2 = tk.Button(tab2, text='Save Settings')
        b2.grid(row=1, column=4, padx=10, pady=10)

        l_title2 = tk.Label(tab2, text='Overrides')
        l_title2.grid(row=6, column=1, padx=10, pady=10)

        c1_bool = tk.BooleanVar(self.window)
        c2_bool = tk.BooleanVar(self.window)
        c3_bool = tk.BooleanVar(self.window)
        c4_bool = tk.BooleanVar(self.window)
        e5_str=tk.StringVar(self.window)
        e6_int=tk.IntVar(self.window)

        c1 = tk.Checkbutton(tab2, text='Palette:', variable=c1_bool, justify='left', anchor='w')
        c1.grid(row=7, column=1, sticky='w', padx=10)
        c2 = tk.Checkbutton(tab2, text='Subdivisions:', variable=c2_bool, justify='left', anchor='w')
        c2.grid(row=8, column=1, sticky='w', padx=10)
        c3 = tk.Checkbutton(tab2, text='Flatten Vertex Colors', variable=c3_bool, justify='left', anchor='w')
        c3.grid(row=9, column=1, sticky='w', padx=10)
        c4 = tk.Checkbutton(tab2, text='Debug', variable=c4_bool, justify='left', anchor='w')
        c4.grid(row=10, column=1, sticky='w', padx=10)

        e5 = tk.Entry(tab2, textvariable=e5_str, width=20, justify='left')
        e5.grid(row=7, column=2, sticky='w')
        e6 = tk.Entry(tab2, textvariable=e6_int, width=3, justify='left')
        e6.grid(row=8, column=2, sticky='w')

        # Network Tab ---------------------------------------------------------
        l_title3 = tk.Label(tab3, text='Distributed Processing')
        l_title3.grid(row=1, column=1, padx=10, pady=10)

        c5_bool = tk.BooleanVar(self.window)
        c6_bool = tk.BooleanVar(self.window)
        e7_int=tk.IntVar(self.window)

        c5 = tk.Checkbutton(tab3, text='Share CPU Cores:', variable=c5_bool, justify='left', anchor='w')
        c5.grid(row=2, column=1, sticky='w', padx=10)
        c6 = tk.Checkbutton(tab3, text='Use Network Nodes', variable=c6_bool, justify='left', anchor='w')
        c6.grid(row=3, column=1, sticky='w', padx=10)

        e7 = tk.Entry(tab3, textvariable=e7_int, width=3, justify='left')
        e7.grid(row=2, column=2, sticky='w')

        l_title4 = tk.Label(tab3, text='Node Discovery')
        l_title4.grid(row=4, column=1, padx=10, pady=10)


        def table(root, data, startrow, startcolumn):
            rows = len(data)
            columns = len(data[0])
            for i in range(rows):
                for j in range(columns):
                    self.e = tk.Entry(root)
                    self.e.grid(row=i+startrow, column=j+startcolumn)
                    self.e.insert('end', data[i][j])
 
        # ip, hostname, cores, os
        data = [('192.168.0.100','doc','linux',32),
            ('192.168.0.101','grumpy','win',16),
            ('192.168.0.102','sleepy','mac',8),
            ('192.168.0.103','bashful','linux',8),
            ('192.168.0.104','happy','win',10),
            ('192.168.0.105','sneezy','mac',6),
            ('192.168.0.106','dopey','win',4)]

        table(tab3, data, 5, 1)


        self.window.mainloop()


class SXBATCHER_process(object):
    def __init__(self):
        return None


    def __del__(self):
        print('SX Batcher: Exiting process')


    def get_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-o', '--open', default=sxglobals.catalogue_path, help='Open a Catalogue file')
        parser.add_argument('-a', '--all', action='store_true', help='Export the entire Catalogue')
        parser.add_argument('-r', '--remotetask', nargs='+', type=str, help='Process list of files (distributed mode)')
        parser.add_argument('-d', '--folder', help='Ignore the Catalogue, export all objects from a folder')
        parser.add_argument('-c', '--category', help='Export all objects in a category (Default, Paletted...')
        parser.add_argument('-f', '--filename', help='Export an object by filename')
        parser.add_argument('-t', '--tag', help='Export all tagged objects')
        parser.add_argument('-s', '--sxtools', default=sxglobals.sxtools_path, help='SX Tools folder')
        parser.add_argument('-sd', '--subdivision', type=str, help='SX Tools subdivision override')
        parser.add_argument('-sp', '--palette', type=str, help='SX Tools palette override')
        parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='SX Tools flatten layers to VertexColor0')
        parser.add_argument('-e', '--exportpath', default=sxglobals.export_path, help='Export path')
        parser.add_argument('-l', '--listonly', action='store_true', help='Do not export, only list objects that match the other arguments')
        parser.add_argument('-v', '--verbose', action='store_true', help='Display debug messages')
        parser.add_argument('-u', '--updaterepo', action='store_true', help='Update art asset repository to the latest version (PlasticSCM)')
        all_arguments, ignored = parser.parse_known_args()
        return all_arguments


    def sx_batch_process(self, process_args):
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

        if sxglobals.debug:
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
            for line in lines:
                if debug:
                    print(line)
                else:
                    if 'Error' in line:
                        print(line)
        except subprocess.CalledProcessError as error:
            print('SX Batch Error:', source_file)


    def export_selected(self):
        asset_path = None
        args = process.get_args()

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
            asset_dict = init.load_asset_data(catalogue_path)

        # grab blender work script from the location of this script
        script_path = str(os.path.realpath(__file__)).replace(os.path.basename(__file__), 'sx_batch.py')
        source_files = []

        # Build source file list according to arguments
        for obj in sxglobals.export_objs:
            for category in sxglobals.catalogue.keys():
                for key, values in sxglobals.catalogue[category].items():
                    for value in values:
                        if obj == value:
                            file_path = key.replace('//', os.path.sep)
                            source_files.append(os.path.join(asset_path, file_path))

        # Construct node-specific task assignment list
        if len(source_files) > 0:
            source_files = list(set(source_files))
            print('\n' + sxglobals.nodename + ': Source files:')
            for file in source_files:
                print(file)

        # Generate task definition for each headless Blender
        tasks = []
        for file in source_files:
            tasks.append(
                (sxglobals.blender_path,
                file,
                script_path,
                os.path.abspath(sxglobals.export_path),
                os.path.abspath(sxglobals.sxtools_path),
                subdivision,
                palette,
                staticvertexcolors,
                debug))


        num_cores = multiprocessing.cpu_count()

        then = time.time()
        print(sxglobals.nodename + ': Spawning workers')

        with Pool(processes=num_cores, maxtasksperchild=1) as pool:
            for i, _ in enumerate(pool.imap(self.sx_batch_process, tasks)):
                print(sxglobals.nodename + ': Progress {0}%'.format(round(i/len(tasks)*100)))

        now = time.time()
        print(sxglobals.nodename + ':', len(source_files), 'files exported in', round(now-then, 2), 'seconds\n')


# ------------------------------------------------------------------------
#    NOTE: The catalogue file should be located in the root
#          of your asset folder structure.
# ------------------------------------------------------------------------

sxglobals = SXBATCHER_globals()
init = SXBATCHER_init()
gui = SXBATCHER_gui()
process = SXBATCHER_process()

if __name__ == '__main__':
    sxglobals.ip_addr = init.get_ip()
    sxglobals.nodename = 'Node '+sxglobals.ip_addr

    init.load_conf()
    sxglobals.catalogue = init.load_asset_data(sxglobals.catalogue_path)
    sxglobals.categories = list(sxglobals.catalogue.keys())
    sxglobals.category = sxglobals.categories[0]
    gui.draw_window()
