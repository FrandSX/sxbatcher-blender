bl_info = {
    'name': 'SX Batcher',
    'author': 'Jani Kahrama / Secret Exit Ltd.',
    'version': (1, 2, 2),
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
#    Globals
# ------------------------------------------------------------------------
class SXBATCHER_sxglobals(object):
    def __init__(self):
        self.catalogue = {}
        # Use absolute paths
        bpy.context.preferences.filepaths.use_relative_paths = False


    def __del__(self):
        print('SX Batcher: Exiting sxglobals')


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


def load_catalogue():
    catalogue_path = bpy.context.preferences.addons['sxbatcher'].preferences.cataloguepath
    if os.path.isfile(catalogue_path):
        try:
            with open(catalogue_path, 'r') as input:
                temp_dict = {}
                temp_dict = json.load(input)
                input.close()
            sxglobals.catalogue = temp_dict
        except ValueError:
            message_box('Invalid Catalogue file. Creating empty template.', 'SX Batcher Error', 'ERROR')
            sxglobals.catalogue = {}
        except IOError:
            message_box('Catalogue file not found. Creating empty template', 'SX Batcher Error', 'ERROR')
            sxglobals.catalogue = {}
    else:
        message_box('Creating new catalogue')
        sxglobals.catalogue = {}
        save_catalogue()

    return sxglobals.catalogue


def save_catalogue():
    catalogue_path = bpy.context.preferences.addons['sxbatcher'].preferences.cataloguepath
    if len(catalogue_path) > 0:
        with open(catalogue_path, 'w') as output:
            json.dump(sxglobals.catalogue, output, indent=4)
            output.close()
        message_box(catalogue_path + ' saved')
    else:
        message_box('Catalogue path not set!', 'SX Batcher Error', 'ERROR')


def dict_lister(self, context):
    data_dict = load_catalogue()
    items = data_dict.keys()
    enumItems = []
    for item in items:
        enumItem = (item.replace(" ", "_").upper(), item+' ('+str(len(data_dict[item]))+' files)', '')
        enumItems.append(enumItem)
    return enumItems


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


class SXBATCHER_sceneprops(bpy.types.PropertyGroup):
    category: bpy.props.EnumProperty(
        name='Category',
        description='Select object category',
        items=lambda self, context: dict_lister(self, context))


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
        prefs = bpy.context.preferences.addons['sxbatcher'].preferences
        layout = self.layout
        scene = context.scene.sxbatcher
        catalogue_size = 0
        if len(prefs.cataloguepath) > 0:
            if len(sxglobals.catalogue) > 0:
                for category in sxglobals.catalogue:
                    catalogue_size += len(sxglobals.catalogue[category])

            row_category = layout.row(align=True)
            row_category.prop(scene, 'category', text='')
            row_category.operator('sxbatcher.category_add', text='', icon='ADD')
            row_category.operator('sxbatcher.category_remove', text='', icon='REMOVE')
            row_stats = layout.row(align=True)
            row_stats.label(text=f'Catalogue files: {catalogue_size}')
            layout.separator()
            col_catalogue = layout.column(align=True)
            col_catalogue.operator('sxbatcher.catalogue_add', text='Add File to Catalogue', icon='ADD')
            col_catalogue.operator('sxbatcher.catalogue_remove', text='Remove File from Catalogue', icon='REMOVE')
        else:
            layout.label(text='Invalid catalogue file path in addon settings')


class SXBATCHER_OT_category_add(bpy.types.Operator):
    bl_idname = 'sxbatcher.category_add'
    bl_label = 'Add Asset Category'
    bl_description = 'Add a category to the asset catalogue'

    cat_name: bpy.props.StringProperty(name='Category Name')


    def invoke(self, context, event):
        return context.window_manager.invoke_props_dialog(self)


    def draw(self, context):
        layout = self.layout
        col = layout.column()
        col.prop(self, 'cat_name', text='')


    def execute(self, context):
        category_name = self.cat_name.lower()
        found = False
        for category in sxglobals.catalogue:
            if category == category_name:
                message_box('Category already exists')
                found = True
                break

        if not found:
            cat_enum = self.cat_name.replace(" ", "_").upper()
            sxglobals.catalogue[category_name] = {}
            save_catalogue()
            context.scene.sxbatcher.category = cat_enum
        return {'FINISHED'}


class SXBATCHER_OT_category_remove(bpy.types.Operator):
    bl_idname = 'sxbatcher.category_remove'
    bl_label = 'Remove Asset Category'
    bl_description = 'Remove a category from the asset catalogue'


    def invoke(self, context, event):
        cat_enum = context.scene.sxbatcher.category[:]
        cat_name = cat_enum.replace("_", " ").lower()

        for category in sxglobals.catalogue:
            if category == cat_name:
                sxglobals.catalogue.pop(category)
                break

        save_catalogue()
        categories = list(sxglobals.catalogue.keys())
        cat = categories[0].replace(' ', '_').upper()
        context.scene.sxbatcher.category = cat
        return {'FINISHED'}


class SXBATCHER_OT_catalogue_add(bpy.types.Operator):
    bl_idname = 'sxbatcher.catalogue_add'
    bl_label = 'Add File to Asset Catalogue'
    bl_description = 'Add current file to the Asset Catalogue for batch exporting'

    assetTags: bpy.props.StringProperty(name='Tags')


    def check_existing(self, context):
        prefs = bpy.context.preferences.addons['sxbatcher'].preferences
        file_path = bpy.data.filepath
        asset_path = os.path.split(prefs.cataloguepath)[0]

        for category in sxglobals.catalogue.keys():
            for key in sxglobals.catalogue[category].keys():
                key_path = key.replace('//', os.path.sep)
                if os.path.samefile(file_path, os.path.join(asset_path, key_path)):
                    return True
        return False


    # Check if file is located under the specified folder
    def check_location(self, context):
        prefs = context.preferences.addons['sxbatcher'].preferences
        file_path = bpy.data.filepath
        asset_path = os.path.split(prefs.cataloguepath)[0]
        prefix = os.path.commonpath([asset_path, file_path])
        if not os.path.samefile(asset_path, prefix):
            message_box('File not located under asset folders!', 'SX Batcher Error', 'ERROR')
            return False
        else:
            return True


    def check_saved(self, context):
        if len(bpy.data.filepath) == 0:
            message_box('Current file has not been saved!', 'SX Batcher Error', 'ERROR')
            return False
        else:
            return True


    def invoke(self, context, event):
        # Check if the open scene has been saved to a file
        if self.check_saved(context) and self.check_location(context):
            return context.window_manager.invoke_props_dialog(self)
        else:
            return {'FINISHED'}


    def draw(self, context):
        layout = self.layout
        col = layout.column()
        col.prop(self, 'assetTags')
        col.label(text='Use only spaces between multiple tags')
        if self.check_existing(context):
            col.label(text='File already catalogue, existing tags will be overwritten')


    def execute(self, context):
        prefs = context.preferences.addons['sxbatcher'].preferences
        asset_category = context.scene.sxbatcher.category.replace("_", " ").lower()
        asset_tags = self.assetTags.split(' ')
        file_path = bpy.data.filepath
        asset_path = os.path.split(prefs.cataloguepath)[0]
        file_rel_path = os.path.relpath(file_path, asset_path)

        # Store category into objects and highest revision in the scene into catalogue
        revision = 1
        for obj in bpy.data.objects:
            if obj.type == 'MESH':
                obj['category'] = asset_category
                if ('revision' not in obj.keys()):
                    obj['revision'] = 1
                elif obj['revision'] > revision:
                    revision = obj['revision']

        # Update cost (vert count)
        s = context.scene.statistics(context.view_layer)
        cost = s.split("Tris:")[1].split(' ')[0].replace(',', '')

        # Add asset to category and save entry with a platform-independent path separator
        objs = [obj.name for obj in context.view_layer.objects if obj.type == 'MESH']
        sxglobals.catalogue[asset_category][file_rel_path.replace(os.path.sep, '//')] = {'tags': asset_tags, 'objects': objs, 'revision': str(revision), 'cost': cost}
        save_catalogue()
        return {'FINISHED'}


class SXBATCHER_OT_catalogue_remove(bpy.types.Operator):
    bl_idname = 'sxbatcher.catalogue_remove'
    bl_label = 'Remove File from Catalogue'
    bl_description = 'Remove current file from Catalogue'


    def invoke(self, context, event):
        prefs = bpy.context.preferences.addons['sxbatcher'].preferences
        file_path = bpy.data.filepath
        if len(file_path) == 0:
            message_box('Current file has not been saved!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        asset_path = os.path.split(prefs.cataloguepath)[0]
        paths = [asset_path, file_path]
        prefix = os.path.commonpath(paths)
        file_rel_path = os.path.relpath(file_path, asset_path)

        if not os.path.samefile(asset_path, prefix):
            message_box('File not located under asset folders!', 'SX Batcher Error', 'ERROR')
            return {'FINISHED'}

        for asset_category in sxglobals.catalogue:
            sxglobals.catalogue[asset_category].pop(file_rel_path.replace(os.path.sep, '//'), None)

        save_catalogue()
        return {'FINISHED'}


# ------------------------------------------------------------------------
#    Registration and initialization
# ------------------------------------------------------------------------
sxglobals = SXBATCHER_sxglobals()

classes = (
    SXBATCHER_preferences,
    SXBATCHER_sceneprops,
    SXBATCHER_PT_panel,
    SXBATCHER_OT_category_add,
    SXBATCHER_OT_category_remove,
    SXBATCHER_OT_catalogue_add,
    SXBATCHER_OT_catalogue_remove)


def register():
    from bpy.utils import register_class
    for cls in classes:
        register_class(cls)

    bpy.types.Scene.sxbatcher = bpy.props.PointerProperty(type=SXBATCHER_sceneprops)

    bpy.app.handlers.save_pre.append(save_pre_handler)    
    bpy.app.handlers.save_post.append(save_post_handler)


def unregister():
    from bpy.utils import unregister_class
    for cls in reversed(classes):
        unregister_class(cls)

    del bpy.types.Scene.sxbatcher

    bpy.app.handlers.save_pre.remove(save_pre_handler)
    bpy.app.handlers.save_post.remove(save_post_handler)


if __name__ == '__main__':
    try:
        unregister()
    except:
        pass
    register()
