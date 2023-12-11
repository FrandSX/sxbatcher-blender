import argparse
import bpy
import os
import sys

def get_args():
    parser = argparse.ArgumentParser()

    # get all script args
    _, all_arguments = parser.parse_known_args()
    double_dash_index = all_arguments.index('--')
    script_args = all_arguments[double_dash_index + 1: ]

    # add parser rules
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
#    The below steps are designed for use with SX Tools 2 Blender addon.
#    Edit according to the needs of your project.
# ------------------------------------------------------------------------

# bpy.ops.wm.addon_install(filepath='/home/bob/sxtools-blender/sxtools.py')
bpy.ops.preferences.addon_enable(module="sxtools2")
bpy.context.preferences.addons['sxtools2'].preferences.libraryfolder = library_path
bpy.context.preferences.addons['sxtools2'].preferences.flipsmartx = False
bpy.context.preferences.addons['sxtools2'].preferences.exportspace = 'LIN'
bpy.context.preferences.addons['sxtools2'].preferences.exportroughness = 'SMOOTH'
bpy.data.scenes["Scene"].sx2.exportfolder = export_path
if args.format in ['fbx', 'gltf']:
    bpy.context.preferences.addons['sxtools2'].preferences.exportformat = args.format.upper()

# If objects have legacy sxtools properties, convert to sx2 first
# bpy.ops.object.select_all(action='SELECT')
# bpy.ops.sx2.sxtosx2('EXEC_DEFAULT')

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
