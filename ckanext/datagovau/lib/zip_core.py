import os
import shutil
import time
import uuid
import zipfile
from datetime import datetime

import ckanapi
import requests
from ckan.common import _
from ckan.lib import uploader
from ckanext.datapusher.logic.action import datapusher_status

from ckanext.datagovau.helpers import MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX, check_blacklists, is_zip_resource
from ckanext.datagovau.lib.common import delete_all_children, delete_children, init, update_process_status, \
    update_cleanup_status


# Expands and breaks up a zip file pointed to by the url.
# - Nested Zip files are not immediately expanded. They are saved as zipped resources, with the zip_extract
#   flag set, causing a recursion on the CKAN application level.
# - Sub directories are re-zipped _if_ they contain one or more interesting files/sub-directories
#   and are in a directory with at least one other interesting file/sub-directory
# - Individual, interesting files are moved to the target directory, as needed for upload.
def zip_expand(context, resource):
    def interesting_or_directory(file_name):
        return any([file_name.lower().endswith("." + x.lower()) for x in context['target_zip_formats']] + [
            file_name.endswith("/")])

    def zip_dir(path, zip_handle):
        for root, dirs, files in os.walk(path):
            for f_name in files:
                full_path = os.path.join(root, f_name)
                zip_handle.write(full_path, full_path.replace(path.lstrip('\/'), '').lstrip('\/'))

    def num_interesting_in_dir(dir):
        res = 0
        for root, dirs, files in os.walk(dir):
            for f_name in files:
                if interesting_or_directory(f_name):
                    res += 1

        return res

    def process_contents(dir, target_dir, prefix):
        for root, dirs, files in os.walk(dir):
            num_interest_in_level = 0
            for f_name in files:
                num_interest_in_level += 1
                new_name = prefix + '_' + f_name
                update_process_status(context,
                                      'pending',
                                      "{0} renaming {1} to {2}.".format(MSG_ZIP_PREFIX, f_name, new_name),
                                      resource,
                                      'zipextractor',
                                      None)
                try:
                    shutil.move(os.path.join(dir, f_name), os.path.join(target_dir, new_name))
                except:
                    # Probably righting another thread
                    update_process_status(context,
                                          'error',
                                          "{0} failed to move file, {1}".format(MSG_ZIP_PREFIX,
                                                                                MSG_ZIP_SKIP_SUFFIX),
                                          resource,
                                          'spatialingestor',
                                          "{0} failed to move file {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                                    os.path.join(dir, f_name),
                                                                                    MSG_ZIP_SKIP_SUFFIX))
                    return None
            for sub_dir in dirs:
                if num_interesting_in_dir(os.path.join(root, sub_dir)) > 0:
                    num_interest_in_level += 1
            for sub_dir in dirs:
                if num_interesting_in_dir(os.path.join(root, sub_dir)) > 1 and num_interest_in_level > 1:
                    new_name = prefix + '_' + sub_dir + '.zip'
                    update_process_status(context,
                                          'pending',
                                          "{0} {1} is a non-trivial sub-directory; zipping up as {2}".format(
                                              MSG_ZIP_PREFIX, sub_dir, new_name),
                                          resource,
                                          'zipextractor',
                                          None)

                    try:
                        zip_file = zipfile.ZipFile(os.path.join(target_dir, new_name), 'w',
                                                   zipfile.ZIP_DEFLATED)
                        zip_dir(os.path.join(root, sub_dir), zip_file)
                        zip_file.close()
                    except:
                        # Probably stolen by another thread
                        update_process_status(context,
                                              'error',
                                              "{0} failed to zip up directory, {1}".format(MSG_ZIP_PREFIX,
                                                                                           MSG_ZIP_SKIP_SUFFIX),
                                              resource,
                                              'spatialingestor',
                                              "{0} failed to zip up directory {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                                               os.path.join(root,
                                                                                                            sub_dir),
                                                                                               MSG_ZIP_SKIP_SUFFIX))
                        return None

                    update_process_status(context,
                                          'pending',
                                          "{0} finished zipping sub-directory {1}.".format(MSG_ZIP_PREFIX, sub_dir),
                                          resource,
                                          'zipextractor',
                                          None)
                elif num_interesting_in_dir(os.path.join(root, sub_dir)) > 0:
                    update_process_status(context,
                                          'pending',
                                          "{0} {1} is a trivial sub-directory; recursing past...".format(
                                              MSG_ZIP_PREFIX, sub_dir),
                                          resource,
                                          'zipextractor',
                                          None)
                    # The directory only contains one sub_directory with interesting files. There is no
                    # point compressing this sub directory, so we recurse down into it
                    process_contents(os.path.join(root, sub_dir), target_dir, prefix + '_' + sub_dir)

                try:
                    shutil.rmtree(os.path.join(root, sub_dir))
                except:
                    update_process_status(context,
                                          'error',
                                          "{0} failed to remove directory, {1}".format(MSG_ZIP_PREFIX,
                                                                                       MSG_ZIP_SKIP_SUFFIX),
                                          resource,
                                          'spatialingestor',
                                          "{0} failed to remove directory {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                                           os.path.join(root, sub_dir),
                                                                                           MSG_ZIP_SKIP_SUFFIX))
                    return None

            # Break after one iteration, as any sub-directories will be either Zipped (and recursed into on
            # the application level) or directly recursed into
            break

    random_id = uuid.uuid1()
    tmp_dir = os.path.join(context['temporary_directory'], str(random_id))
    tmp_filepath = None

    if not os.path.isdir(tmp_dir):
        os.makedirs(tmp_dir)
    else:
        # Probably second worker that grabbed the same job
        update_process_status(context,
                              'error',
                              "{0} previous temp directory found, {1}".format(MSG_ZIP_PREFIX,
                                                                              MSG_ZIP_SKIP_SUFFIX),
                              resource,
                              'spatialingestor',
                              "{0} previous temp directory {1} found , {2}".format(MSG_ZIP_PREFIX, tmp_dir,
                                                                                   MSG_ZIP_SKIP_SUFFIX))
        return None

    z = None

    if resource.get('__extras', {}).get('url_type', resource.get('url_type', '')) == 'upload':
        upload = uploader.ResourceUpload(resource)

        try:
            z = zipfile.ZipFile(upload.get_path(resource['id']), 'r')
            tmp_filepath = None
        except Exception, e:
            update_process_status(context,
                                  'error',
                                  "{0} failed to expand local copy of zip, retrying via URL.".format(
                                      MSG_ZIP_PREFIX),
                                  resource,
                                  'zipextractor',
                                  "{0} failed to expand {1} with error {2}, retrying via URL.".format(
                                      MSG_ZIP_PREFIX, upload.get_path(resource['id']), str(e)))
            z = None
            pass

    if z is None:
        response = requests.get(resource['url'].replace('https', 'http'),
                                stream=True,
                                headers={'X-CKAN-API-Key': context['model'].User.get(context['user']).apikey})

        if response.status_code != 200:
            update_process_status(context,
                                  'error',
                                  "{0} {1} could not be downloaded, {2}".format(MSG_ZIP_PREFIX, resource['url'],
                                                                                MSG_ZIP_SKIP_SUFFIX),
                                  resource,
                                  'zipextractor',
                                  None)
            return None

        tmp_name = '{0}.{1}'.format(random_id, 'zip')
        tmp_filepath = os.path.join(tmp_dir, tmp_name)

        try:
            with open(tmp_filepath, 'wb') as out_file:
                response.raw.decode_content = True
                shutil.copyfileobj(response.raw, out_file)
        except:
            # Probably done by another job
            update_process_status(context,
                                  'error',
                                  "{0} failed to copy file, {1}".format(MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX),
                                  resource,
                                  'spatialingestor',
                                  "{0} failed to copy file {1}, {2}".format(MSG_ZIP_PREFIX, out_file,
                                                                            MSG_ZIP_SKIP_SUFFIX))
            return None

        try:
            z = zipfile.ZipFile(tmp_filepath)
        except zipfile.BadZipfile:
            update_process_status(context,
                                  'error',
                                  "{0} {1} is not a valid zip file, {2}".format(MSG_ZIP_PREFIX, resource['url'],
                                                                                MSG_ZIP_SKIP_SUFFIX),
                                  resource,
                                  'zipextractor',
                                  "{0} {1} is not a valid zip file, {2}".format(MSG_ZIP_PREFIX, tmp_filepath,
                                                                                MSG_ZIP_SKIP_SUFFIX))
            return None

    try:
        file_counter = 0
        for entry in z.infolist():
            if interesting_or_directory(entry.filename) and entry.file_size <= context['max_zip_resource_filesize']:
                z.extract(entry, path=tmp_dir)
                file_counter += 1

        if tmp_filepath is not None:
            try:
                os.remove(tmp_filepath)
            except:
                # Other thread probably deleted this
                update_process_status(context,
                                      'error',
                                      "{0} failed to remove temporary zip file, {1}".format(MSG_ZIP_PREFIX,
                                                                                            MSG_ZIP_SKIP_SUFFIX),
                                      resource,
                                      'spatialingestor',
                                      "{0} failed to remove temporary zip file {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                                                tmp_filepath,
                                                                                                MSG_ZIP_SKIP_SUFFIX))
                return None

        update_process_status(context,
                              'pending',
                              "{0} extracted {1} files from Zip. Processing extracted files".format(MSG_ZIP_PREFIX,
                                                                                                    file_counter),
                              resource,
                              'zipextractor',
                              None)

        process_contents(tmp_dir, tmp_dir, resource['name'].split('.', 1)[0])
    except Exception:
        update_process_status(context,
                              'error',
                              "{0} extraction of {1} failed, {2}".format(MSG_ZIP_PREFIX, resource['url'],
                                                                         MSG_ZIP_SKIP_SUFFIX),
                              resource,
                              'zipextractor',
                              None)
        try:
            shutil.rmtree(tmp_dir)
        except:
            # Other worker has most likely deleted the directory
            update_process_status(context,
                                  'error',
                                  "{0} failed to remove directory, {1}".format(MSG_ZIP_PREFIX,
                                                                               MSG_ZIP_SKIP_SUFFIX),
                                  resource,
                                  'spatialingestor',
                                  "{0} failed to remove directory {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                                   tmp_dir,
                                                                                   MSG_ZIP_SKIP_SUFFIX))
        return None

    return tmp_dir


def ingest_dir(context, ckan, tmp_dir, parent_resource):
    for file_name in os.listdir(tmp_dir):
        new_res = {'package_id': parent_resource['package_id'],
                   'url': 'http://blank',
                   'last_modified': datetime.now().isoformat(),
                   'zip_child_of': parent_resource['id'],
                   'parent_resource_url': parent_resource['url']}

        file_path = os.path.join(tmp_dir, file_name)
        new_res['name'] = file_name.split('.', 1)[0]
        new_res['format'] = file_name.split('.')[-1].lower()

        try:
            response_dict = {'result': ckan.call_action('resource_create', data_dict=new_res,
                                                        files=[('upload', open(file_path, 'rb'))])}
        except Exception, e:
            update_process_status(context,
                                  'error',
                                  "{0} {1} could not be uploaded with error {2}, continuing...".format(MSG_ZIP_PREFIX,
                                                                                                       file_name,
                                                                                                       str(e)),
                                  parent_resource,
                                  'zipextractor',
                                  None)

        try:
            # response = requests.post(context['ckan_api_url'] + + "/api/3/action/resource_create",
            #                         data=new_res,
            #                         headers={'X-CKAN-API-Key': context['model'].User.get(context['user']).apikey},
            #                         files=[('upload', open(file_path, 'rb'))])

            # response_dict = response.json()

            # if response_dict['success'] is False:
            #    update_process_status(context,
            #                          'error',
            #                          "{0} {1} could not be uploaded, continuing...".format(MSG_ZIP_PREFIX, file_name),
            #                          parent_resource,
            #                          'zipextractor')
            #    continue

            if context['wait_for_datapusher'] and response_dict['result'].get('format', '').upper() in context[
                'datapusher_default_formats']:
                update_process_status(context,
                                      'pending',
                                      "{0} waiting for datapusher to ingest {1}...".format(MSG_ZIP_PREFIX, file_name),
                                      parent_resource,
                                      'zipextractor',
                                      None)

                # Datapusher will be called on this resource
                status = 'submitting'
                counter = 0
                while status not in ['error', 'complete']:
                    try:
                        status = datapusher_status(context, response_dict['result'])['status']
                    except Exception:
                        update_process_status(context,
                                              'error',
                                              "{0} datapusher threw an error, continuing...".format(MSG_ZIP_PREFIX),
                                              parent_resource,
                                              'zipextractor',
                                              "{0} {1} caused a datapusher error, continuing...".format(MSG_ZIP_PREFIX,
                                                                                                        file_name))
                        break

                    time.sleep(1)
                    counter += 1

                    if (status == 'submitting' and counter >= context['datapusher_submit_timeout']) or \
                            (status != 'submitting' and counter >= context['datapusher_completion_timeout']):
                        update_process_status(context,
                                              'error',
                                              "{0} datapusher timed out, continuing...".format(
                                                  MSG_ZIP_PREFIX,
                                                  file_name),
                                              parent_resource,
                                              'zipextractor',
                                              "{0} {1} caused a datapusher timeout, continuing...".format(
                                                  MSG_ZIP_PREFIX,
                                                  file_name))
                        break
                else:
                    update_process_status(context,
                                          'pending',
                                          "{0} datapusher finished ingesting {1}, continuing...".format(MSG_ZIP_PREFIX,
                                                                                                        file_name),
                                          parent_resource,
                                          'zipextractor',
                                          None)

            # Asynchronous recursion via the celery queue is not reliable, even with monitoring the task status
            # thus we manually recurse in the case of zip-files created from directories
            if is_zip_resource(response_dict['result']):
                process_zip(context, response_dict['result'], False)

        except Exception, e:
            update_process_status(context,
                                  'error',
                                  "{0} failed to create child Zip resource {1}, continuing...".format(
                                      MSG_ZIP_PREFIX,
                                      new_res['name']),
                                  parent_resource,
                                  'zipextractor',
                                  "{0} failed to create child Zip resource {1} with exception {2}, continuing...".format(
                                      MSG_ZIP_PREFIX,
                                      new_res['name'],
                                      str(e)))

    # Remove temp directory, now that resources have been created
    try:
        shutil.rmtree(tmp_dir)
    except:
        # Other worker has most likely deleted the directory
        update_process_status(context,
                              'error',
                              "{0} failed to remove directory, {1}".format(MSG_ZIP_PREFIX,
                                                                           MSG_ZIP_SKIP_SUFFIX),
                              parent_resource,
                              'spatialingestor',
                              "{0} failed to remove directory {1}, {2}".format(MSG_ZIP_PREFIX,
                                                                               tmp_dir,
                                                                               MSG_ZIP_SKIP_SUFFIX))


def process_zip(context, resource, do_init=True):
    if do_init:
        context = init(context, resource, 'zipextract')
        if context is None:
            return

    ckan = ckanapi.RemoteCKAN(address=context['ckan_api_url'],
                              apikey=context['model'].User.get(context['user']).apikey)

    update_process_status(context,
                          'pending',
                          "{0} expanding Zip resource {1}".format(MSG_ZIP_PREFIX, resource['name']),
                          resource,
                          'zipextractor',
                          None)

    res_dir = zip_expand(context, resource)

    # Make sure the download and extraction were successful
    if res_dir is None:
        return

    update_process_status(context,
                          'pending',
                          "{0} deleting pre-existing children of {1}".format(MSG_ZIP_PREFIX, resource['name']),
                          resource,
                          'zipextractor',
                          None)

    # Delete any Zip resources which are the children of this one
    delete_children(context, ckan, resource, 'zip_child_of', MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX, 'zipextractor')

    update_process_status(context,
                          'pending',
                          "{0} creating resource for sub-files of {1}".format(MSG_ZIP_PREFIX, resource['name']),
                          resource,
                          'zipextractor',
                          None)

    ingest_dir(context, ckan, res_dir, resource)

    update_process_status(context,
                          'complete',
                          "{0} all children of {1} ingested.".format(MSG_ZIP_PREFIX,
                                                                     resource.get('name', resource['id'])),
                          resource,
                          'zipextractor',
                          None)


# Only deletes direct children of resource, as these deletions will cause events to be
# passed to the celery queue that will subsequently recall this with children IDs
def zip_delete_all_children(context, resource):
    context = init(context, resource, 'zipextract')
    if context is None:
        return

    delete_children(context, resource, 'zip_child_of', MSG_ZIP_PREFIX, MSG_ZIP_SKIP_SUFFIX, 'zipextractor')

    update_cleanup_status(context,
                          'complete',
                          "{0} all children of {1} deleted.".format(MSG_ZIP_PREFIX,
                                                                    resource.get('name', resource['id'])),
                          resource,
                          'zipextractor',
                          None)


def purge_zip(context, package):
    ckan = ckanapi.RemoteCKAN(address=context['ckan_api_url'],
                              apikey=context['model'].User.get(context['user']).apikey)

    delete_all_children(context, ckan, package, 'zip_child_of', MSG_ZIP_PREFIX, 'zipextractor')
    delete_all_children(context, ckan, package, 'zip_extracted', MSG_ZIP_PREFIX, 'zipextractor')


def rebuild_zip(context, package):
    default_user = context['user']
    for res in package['resources']:
        if is_zip_resource(res):
            context['user'] = res.get('zip_creator', default_user)
            if res.get('name', '') == '':
                res['name'] = _('Unamed resource')

            if res.get('zip_extract', '') == 'True' or (
                            context.get('auto_process', '') == 'True' and check_blacklists(context, package) == ''):
                process_zip(context, res)
