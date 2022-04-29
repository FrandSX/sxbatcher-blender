bl_info = {
    'name': 'SX Batcher',
    'author': 'Jani Kahrama / Secret Exit Ltd.',
    'version': (1, 0, 2),
    'blender': (2, 80, 0),
    'location': 'View3D',
    'description': 'Asset catalogue management tool',
    'doc_url': 'https://www.notion.so/secretexit/SX-Batcher-for-Blender-Documentation-f059e9e8f2694fc99207f0381ccd4688',
    'tracker_url': 'https://github.com/FrandSX/sxbatcher-blender/issues',
    'category': 'Development',
}

import bpy
import json
import os
from bpy.app.handlers import persistent

# ------------------------------------------------------------------------
#    Settings and preferences
# ------------------------------------------------------------------------
class SXBATCHER_preferences(bpy.types.AddonPreferences):
    bl_idname = __name__

    cataloguepath: bpy.props.StringProperty(
        name='Catalogue File',
        description='Catalogue file for batch exporting',
        default='',
        maxlen=1024,
        subtype='FILE_PATH')


    def draw(self, context):
        layout = self.layout
        layout_split = layout.split()
        layout_split.label(text='Catalogue File:')
        layout_split.prop(self, 'cataloguepath', text='')


# ------------------------------------------------------------------------
#    Utils
# ------------------------------------------------------------------------
def message_box(message='', title='SX Batcher', icon='INFO'):
    messageLines = message.splitlines()


    def draw(self, context):
        for line in messageLines:
            self.layout.label(text=line)

    if not bpy.app.background:
        bpy.context.window_manager.popup_menu(draw, title=title, icon=icon)


def load_asset_data(catalogue_path):
    if len(catalogue_path) > 0:
        try:
            with open(catalogue_path, 'r') as input:
                temp_dict = {}
                temp_dict = json.load(input)
                input.close()
            return True, temp_dict
        except ValueError:
            message_box('Invalid Catalogue file. Starting from empty template.', 'SX Batcher Error', 'ERROR')
            return True, {}

        except IOError:
            message_box('Catalogue file not found. Starting from empty template', 'SX Batcher Error', 'ERROR')
            return True, {}
    else:
        message_box('Invalid Catalogue path', 'SX Batcher Error', 'ERROR')
        return False, None


def save_asset_data(catalogue_path, data_dict):
    if len(catalogue_path) > 0:
        with open(catalogue_path, 'w') as output:
            json.dump(data_dict, output, indent=4)
            output.close()
        message_box(catalogue_path + ' saved')
    else:
        message_box('Catalogue path not set!', 'SX Batcher Error', 'ERROR')


# Offset revision prior to file save
@persistent
def save_pre_handler(dummy):
    for obj in bpy.data.objects:
        if obj.type == 'MESH':
            if ('revision' not in obj.keys()):
                obj['revision'] = 1
            else:
                revision = obj['revision']
                obj['revision'] = revision + 1


# On file save, update revision and cost in the asset catalogue
@persistent
def save_post_handler(dummy):
    prefs = bpy.context.preferences.addons['sxbatcher'].preferences
    catalogue_dict = {}
    objs = []
    revision = 1

    for obj in bpy.data.objects:
        if obj.type == 'MESH':
            objs.append(obj)
            if obj['revision'] > revision:
                revision = obj['revision']

    s = bpy.context.scene.statistics(bpy.context.view_layer)
    cost = s.split("Tris:")[1].split(' ')[0].replace(',', '')

    if len(prefs.cataloguepath) > 0:
        try:
            with open(prefs.cataloguepath, 'r') as input:
                catalogue_dict = json.load(input)
                input.close()

            file_path = bpy.data.filepath
            asset_path = os.path.split(prefs.cataloguepath)[0]
            # prefix = os.path.commonpath([asset_path, file_path])
            # file_rel_path = os.path.relpath(file_path, asset_path)

            for category in catalogue_dict:
                for key in catalogue_dict[category]:
                    key_path = key.replace('//', os.path.sep)
                    if os.path.samefile(file_path, os.path.join(asset_path, key_path)):
                        catalogue_dict[category][key]['revision'] = str(revision)
                        catalogue_dict[category][key]['cost'] = cost

            with open(prefs.cataloguepath, 'w') as output:
                json.dump(catalogue_dict, output, indent=4)
                output.close()

        except (ValueError, IOError) as error:
            message_box('Failed to update file revision in Asset Catalogue file.', 'SX Batcher Error', 'ERROR')
            return False, None


# ------------------------------------------------------------------------
#    UI Panel and Pie Menu
# ------------------------------------------------------------------------
class SXBATCHER_PT_panel(bpy.types.Panel):

    bl_idname = 'SXBATCHER_PT_panel'
    bl_label = 'SX Batcher'
    bl_space_type = 'VIEW_3D'
    bl_region_type = 'UI'
    bl_category = 'SX Batcher'


    def draw(self, context):
        prefs = context.preferences.addons['sxbatcher'].preferences
        layout = self.layout

        if (len(prefs.cataloguepath) > 0):
            col_batchexport = layout.column(align=True)
            col_batchexport.operator('sxbatcher.catalogue_add', text='Add to Catalogue', icon='ADD')
            col_batchexport.operator('sxbatcher.catalogue_remove', text='Remove from Catalogue', icon='REMOVE')
        else:
        	layout.label(text='Catalogue path not set!')


class SXBATCHER_OT_catalogue_add(bpy.types.Operator):
    bl_idname = 'sxbatcher.catalogue_add'
    bl_label = 'Add File to Asset Catalogue'
    bl_description = 'Add current file to the Asset Catalogue for batch exporting'

    assetCategory: bpy.props.StringProperty(name='Category')
    assetTags: bpy.props.StringProperty(name='Tags')


    def check_existing(self, context):
        prefs = context.preferences.addons['sxbatcher'].preferences
        file_path = bpy.data.filepath
        asset_path = os.path.split(prefs.cataloguepath)[0]
        asset_dict = {}
        result, asset_dict = load_asset_data(prefs.cataloguepath)
        if not result:
            return {'FINISHED'}
        for category in asset_dict.keys():
            for key in asset_dict[category].keys():
                key_path = key.replace('//', os.path.sep)
                if os.path.samefile(file_path, os.path.join(asset_path, key_path)):
                    return True
        return False


    def check_saved(self, context):
        if len(bpy.data.filepath) == 0:
            message_box('Current file has not been saved!', 'SX Batcher Error', 'ERROR')
            return False
        else:
            return True


    def invoke(self, context, event):
        if self.check_saved(context):
            return context.window_manager.invoke_props_dialog(self)
        else:
            return {'FINISHED'}


    def draw(self, context):
        layout = self.layout
        col = layout.column()
        col.prop(self, 'assetCategory')
        col.prop(self, 'assetTags')
        col.label(text='Use only spaces between multiple tags')
        if self.check_existing(context):
            col.label(text='File already catalogue, existing tags will be overwritten')


    def execute(self, context):
        asset_dict = {}
        prefs = context.preferences.addons['sxbatcher'].preferences
        result, asset_dict = load_asset_data(prefs.cataloguepath)
        if not result:
            return {'FINISHED'}

        asset_category = self.assetCategory
        asset_tags = self.assetTags.split(' ')
        file_path = bpy.data.filepath

        # Check if the open scene has been saved to a file
        if len(file_path) == 0:
            message_box('Current file has not been saved!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        asset_path = os.path.split(prefs.cataloguepath)[0]
        prefix = os.path.commonpath([asset_path, file_path])
        file_rel_path = os.path.relpath(file_path, asset_path)

        # Check if file is located under the specified folder
        if not os.path.samefile(asset_path, prefix):
            message_box('File not located under asset folders!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        # Check if the Catalogue already contains the asset category
        if asset_category not in asset_dict.keys():
            asset_dict[asset_category] = {}

        # Store highest revision in the scene into catalogue
        revision = 1
        for obj in bpy.data.objects:
            if obj.type == 'MESH':
                if ('revision' not in obj.keys()):
                    obj['revision'] = 1
                elif obj['revision'] > revision:
                    revision = obj['revision']

        # Update cost (vert count)
        s = context.scene.statistics(context.view_layer)
        cost = s.split("Tris:")[1].split(' ')[0].replace(',', '')

        # Add asset to category
        objs = [obj.name for obj in context.view_layer.objects if obj.type == 'MESH']
        asset_dict[asset_category][file_rel_path.replace(os.path.sep, '//')] = {'tags': asset_tags, 'objects': objs, 'revision': str(revision), 'cost': cost}

        # Save entry with a platform-independent path separator
        save_asset_data(prefs.cataloguepath, asset_dict)
        return {'FINISHED'}


class SXBATCHER_OT_catalogue_remove(bpy.types.Operator):
    bl_idname = 'sxbatcher.catalogue_remove'
    bl_label = 'Remove File from Catalogue'
    bl_description = 'Remove current file from Catalogue'


    def invoke(self, context, event):
        prefs = context.preferences.addons['sxbatcher'].preferences
        result, asset_dict = load_asset_data(prefs.cataloguepath)
        if not result:
            return {'FINISHED'}

        file_path = bpy.data.filepath
        if len(file_path) == 0:
            message_box('Current file not saved!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        asset_path = os.path.split(prefs.cataloguepath)[0]
        paths = [asset_path, file_path]
        prefix = os.path.commonpath(paths)

        file_rel_path = os.path.relpath(file_path, asset_path)

        if not os.path.samefile(asset_path, prefix):
            message_box('File not located under asset folders!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        for asset_category in asset_dict.keys():
            asset_dict[asset_category].pop(file_rel_path.replace(os.path.sep, '//'), None)

        save_asset_data(prefs.cataloguepath, asset_dict)
        return {'FINISHED'}


classes = (
    SXBATCHER_preferences,
    SXBATCHER_PT_panel,
    SXBATCHER_OT_catalogue_add,
    SXBATCHER_OT_catalogue_remove)


def register():
    from bpy.utils import register_class
    for cls in classes:
        register_class(cls)

    bpy.app.handlers.save_pre.append(save_pre_handler)    
    bpy.app.handlers.save_post.append(save_post_handler)


def unregister():
    from bpy.utils import unregister_class
    for cls in reversed(classes):
        unregister_class(cls)

    bpy.app.handlers.save_pre.remove(save_pre_handler)
    bpy.app.handlers.save_post.remove(save_post_handler)


if __name__ == '__main__':
    try:
        unregister()
    except:
        pass
    register()

    # Use absolute paths
    bpy.context.preferences.filepaths.use_relative_paths = False
