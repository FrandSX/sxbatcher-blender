import argparse
import bpy
import os
import pathlib
import sys

def get_args():
    parser = argparse.ArgumentParser()

    # get all script args
    _, all_arguments = parser.parse_known_args()
    double_dash_index = all_arguments.index('--')
    script_args = all_arguments[double_dash_index + 1: ]

    # add parser rules
    parser.add_argument('-x', '--exportpath', help='Export Path')
    parsed_script_args, _ = parser.parse_known_args(script_args)
    return parsed_script_args

args = get_args()
export_path = os.path.realpath(args.exportpath)

# ------------------------------------------------------------------------
#    The below script is passed to each headless Blender
#    to process the batched blend file.
#    Edit according to the needs of your project.
# ------------------------------------------------------------------------

# 1) Select the files to be processed
bpy.ops.object.select_all(action='SELECT')
objs = bpy.context.view_layer.objects.selected

# 2) Run the batch operations
for obj in objs:
    # bake steps here, or remove obj loop and bake all at once
    pass

# 3) Export fbx files
if len(objs) > 0:
    for obj in objs:
        bpy.context.view_layer.objects.active = obj
        bpy.ops.object.select_all(action='DESELECT')
        obj.select_set(True)

        # Determine category from custom property assigned in Blender
        category = obj.get('category', '')

        print(f'Determining path: {obj.name} {category}')
        path = os.path.join(export_path, category)
        pathlib.Path(path).mkdir(exist_ok=True)

        export_loc = str(os.path.join(path, obj.name+'.fbx'))
        export_settings = ['FBX_SCALE_UNITS', False, False, False, 'Z', '-Y', '-Y', '-X']

        bpy.ops.export_scene.fbx(
            filepath=export_loc,
            apply_scale_options=export_settings[0],
            use_selection=True,
            apply_unit_scale=export_settings[1],
            use_space_transform=export_settings[2],
            bake_space_transform=export_settings[3],
            use_mesh_modifiers=True,
            axis_up=export_settings[4],
            axis_forward=export_settings[5],
            use_active_collection=False,
            add_leaf_bones=False,
            primary_bone_axis=export_settings[6],
            secondary_bone_axis=export_settings[7],
            object_types={'ARMATURE', 'EMPTY', 'MESH'},
            use_custom_props=True,
            use_metadata=False)

        print(f'Completed: {obj.name}')
else:
    print('No objects to export')

bpy.ops.wm.quit_blender('EXEC_DEFAULT')
