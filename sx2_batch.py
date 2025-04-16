import argparse
import bpy
import os
import sys
import addon_utils


def get_args():
    parser = argparse.ArgumentParser()

    # get all script args
    _, all_arguments = parser.parse_known_args()
    double_dash_index = all_arguments.index('--')
    script_args = all_arguments[double_dash_index + 1: ]

    # add parser rules
    parser.add_argument('-sx', '--sxaddonpath', help='Path to SX Tools 2 addon')
    parser.add_argument('-x', '--exportpath', help='Export Path')
    parser.add_argument('-f', '--format', help='Export File Format')
    parser.add_argument('-l', '--librarypath', help='SX Tools Library Path')
    parser.add_argument('-sd', '--subdivision', help='Subdivision Level Override')
    parser.add_argument('-sp', '--palette', help='Palette Override')
    parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='Flatten layers to VertexColor0')
    parser.add_argument('-co', '--collideroffset', help='Convex Hull Shrink Offset')
    parsed_script_args, _ = parser.parse_known_args(script_args)
    return parsed_script_args

args = get_args()
export_path = os.path.abspath(args.exportpath) + os.path.sep
library_path = os.path.abspath(args.librarypath) + os.path.sep


# ------------------------------------------------------------------------
#    Check if SX Tools 2 is already installed
# ------------------------------------------------------------------------
def check_installed():
    for mod in addon_utils.modules():
        if mod.bl_info.get('name') == 'SX Tools 2':
            return True
    return False


def install_addon(addon_path):
    if os.path.isfile(addon_path) and addon_path.endswith('.py'):
        print(f'Installing SX Tools 2 from Python file: {addon_path}')
        bpy.ops.preferences.addon_install(filepath=addon_path)
        bpy.ops.preferences.addon_enable(module='sxtools2')    
    # For a directory, look for sxtools2.py
    elif os.path.isdir(addon_path):
        main_file = os.path.join(addon_path, 'sxtools2.py')
        if os.path.exists(main_file):
            print(f'Installing SX Tools 2 from Python file: {main_file}')
            bpy.ops.preferences.addon_install(filepath=main_file)
            bpy.ops.preferences.addon_enable(module='sxtools2')
        else:
            print(f'Error: Could not find sxtools2.py in {addon_path}')
            sys.exit(1)
    else:
        print(f'Error: Invalid addon path: {addon_path}')
        sys.exit(1)


if check_installed():
    try:
        bpy.ops.preferences.addon_enable(module='sxtools2')
        print("SX Tools 2 installed and enabled.")
    except Exception as e:
        print(f"Error enabling SX Tools 2: {e}")
else:
    print("SX Tools 2 not found. Attempting to install.")
    if args.sxaddonpath:
        addon_path = os.path.abspath(args.sxaddonpath)
        install_addon(addon_path)
    else:
        print('Error: SX Tools 2 is not installed and no addon path provided.')
        print('Please provide the path to SX Tools 2 addon with -sx/--sxaddonpath parameter.')
        sys.exit(1)


# ------------------------------------------------------------------------
#    The below steps are designed for use with SX Tools 2 Blender addon.
#    Edit according to the needs of your project.
# ------------------------------------------------------------------------

bpy.context.preferences.addons['sxtools2'].preferences.libraryfolder = library_path
bpy.context.preferences.addons['sxtools2'].preferences.flipsmartx = False
bpy.context.preferences.addons['sxtools2'].preferences.exportspace = 'LIN'
bpy.context.preferences.addons['sxtools2'].preferences.exportroughness = 'SMOOTH'
bpy.data.scenes['Scene'].sx2.exportfolder = export_path
if args.format in ['fbx', 'gltf']:
    bpy.context.preferences.addons['sxtools2'].preferences.exportformat = args.format.upper()

# If objects have legacy sxtools properties, convert to sx2 first
# bpy.ops.object.select_all(action='SELECT')
# bpy.ops.sx2.sxtosx2('EXEC_DEFAULT')

bpy.ops.object.mode_set(mode='OBJECT', toggle=False)
bpy.ops.object.select_all(action='SELECT')

if args.subdivision is not None:
    subdivision = int(args.subdivision)
    for obj in bpy.context.view_layer.objects.selected:
        if 'sx2' in obj.keys():
            obj.sx2.subdivisionlevel = subdivision

if (args.palette is not None):
    if args.palette in bpy.context.scene.sxpalettes.keys():
        palette = str(args.palette)
        bpy.ops.sx2.applypalette('EXEC_DEFAULT', label=palette)
    else:
        print('SX Batch: Invalid palette name!')

if args.staticvertexcolors is not None:
    for obj in bpy.context.view_layer.objects.selected:
        if 'sx2' in obj.keys():
            obj.sx2.staticvertexcolors = str(int(bool(args.staticvertexcolors)))

if args.collideroffset is not None:
    for obj in bpy.context.view_layer.objects.selected:
        if 'sx2' in obj.keys():
            obj.sx2.collideroffsetfactor = float(args.collideroffset)

for obj in bpy.context.view_layer.objects.selected:
    if 'sx2' in obj.keys():
        obj['sxToolsVersion'] = 'SX Tools 2 for Blender ' + str(sys.modules['sxtools2'].bl_info.get('version'))

# Disable mesh poly count optimizations
for obj in bpy.context.view_layer.objects.selected:
    if 'sxWeld' in obj.modifiers.keys():
        obj.modifiers['sxWeld'].show_viewport = False

    if 'sxDecimate' in obj.modifiers.keys():
            obj.modifiers['sxDecimate'].show_viewport = False

    if 'sxDecimate2' in obj.modifiers.keys():
            obj.modifiers['sxDecimate2'].show_viewport = False

bpy.ops.sx2.macro('EXEC_DEFAULT')
bpy.ops.sx2.exportfiles('EXEC_DEFAULT')

bpy.ops.wm.quit_blender('EXEC_DEFAULT')
