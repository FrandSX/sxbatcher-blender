import argparse
import bpy
import os

def get_args():
    parser = argparse.ArgumentParser()

    # get all script args
    _, all_arguments = parser.parse_known_args()
    double_dash_index = all_arguments.index('--')
    script_args = all_arguments[double_dash_index + 1: ]

    # add parser rules
    parser.add_argument('-x', '--exportpath', help='Export Path')
    parser.add_argument('-l', '--librarypath', help='SX Tools Library Path')
    parser.add_argument('-sd', '--subdivision', help='Subdivision Level Override')
    parser.add_argument('-sp', '--palette', help='Palette Override')
    parser.add_argument('-st', '--staticvertexcolors', action='store_true', help='Flatten layers to VertexColor0')
    parsed_script_args, _ = parser.parse_known_args(script_args)
    return parsed_script_args

args = get_args()
export_path = os.path.abspath(args.exportpath) + os.path.sep
library_path = os.path.abspath(args.librarypath) + os.path.sep

# ------------------------------------------------------------------------
#    The below steps are designed for use with SX Tools Blender addon.
#    Edit according to the needs of your project.
# ------------------------------------------------------------------------
bpy.ops.preferences.addon_enable(module="sxtools")
bpy.context.preferences.addons['sxtools'].preferences.libraryfolder = library_path
# bpy.ops.sxtools.loadlibraries('EXEC_DEFAULT')

bpy.data.scenes["Scene"].sxtools.exportfolder = export_path

bpy.ops.object.select_all(action='SELECT')

if args.subdivision is not None:
    subdivision = int(args.subdivision)
    for obj in bpy.context.view_layer.objects.selected:
        if 'sxtools' in obj.keys():
            obj.sxtools.subdivisionlevel = subdivision

if (args.palette is not None):
    if args.palette in bpy.context.scene.sxpalettes.keys():
        palette = str(args.palette)
        bpy.ops.sxtools.applypalette('EXEC_DEFAULT', label=palette)
    else:
        print('SX Batch: Invalid palette name!')


if args.staticvertexcolors is not None:
    for obj in bpy.context.view_layer.objects.selected:
        if 'sxtools' in obj.keys():
            obj['staticVertexColors'] = bool(args.staticvertexcolors)

bpy.ops.sxtools.macro('EXEC_DEFAULT')
bpy.ops.sxtools.exportfiles('EXEC_DEFAULT')

bpy.ops.wm.quit_blender('EXEC_DEFAULT')
