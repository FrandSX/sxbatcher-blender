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
#    The below steps are designed for use with SX Tools 2 Blender addon.
#    Edit according to the needs of your project.
# ------------------------------------------------------------------------
bpy.ops.preferences.addon_enable(module="sxtools2")
bpy.context.preferences.addons['sxtools2'].preferences.libraryfolder = library_path
bpy.context.preferences.addons['sxtools2'].preferences.flipsmartx = False
bpy.context.preferences.addons['sxtools2'].preferences.exportspace = 'LIN'
bpy.context.preferences.addons['sxtools2'].preferences.exportroughness = 'SMOOTH'
bpy.data.scenes["Scene"].sx2.exportfolder = export_path

bpy.ops.object.select_all(action='SELECT')
# If object has sxtools properties, convert
bpy.ops.sx2.sxtosx2('EXEC_DEFAULT')
bpy.ops.object.select_all(action='SELECT')

for obj in bpy.context.view_layer.objects.selected:
    if 'sx2' in obj.keys():
        obj['sxToolsVersion'] = 'SX Tools 2 for Blender ' + str(sys.modules['sxtools2'].bl_info.get('version'))

bpy.ops.wm.save_mainfile('EXEC_DEFAULT')
bpy.ops.wm.quit_blender('EXEC_DEFAULT')
