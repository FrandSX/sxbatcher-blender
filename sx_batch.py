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
	parser.add_argument('-x', '--exportpath', help="Export Path")
	parsed_script_args, _ = parser.parse_known_args(script_args)
	return parsed_script_args

args = get_args()
export_path = os.path.abspath(args.exportpath) + os.path.sep

# ------------------------------------------------------------------------
#    The below steps are designed for use with SX Tools Blender addon.
#	 Edit according to the needs of your project.
# ------------------------------------------------------------------------
bpy.ops.preferences.addon_enable(module="sxtools")
bpy.context.preferences.addons['sxtools'].preferences.libraryfolder = '/Users/bob/sxtools-blender/' # <- Your sxtools folder here!
# bpy.ops.sxtools.loadlibraries('EXEC_DEFAULT')

bpy.data.scenes["Scene"].sxtools.exportfolder = export_path

bpy.ops.object.select_all(action='SELECT')
bpy.ops.sxtools.macro('EXEC_DEFAULT')

bpy.ops.sxtools.exportfiles('EXEC_DEFAULT')

bpy.ops.wm.quit_blender('EXEC_DEFAULT')