# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2013-2014 CloudVPS
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
"""
Driver for Linux servers running ZFS.

"""

import math
import socket
import time

from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import importutils

from cinder import exception
from cinder.image import image_utils
#from cinder.openstack.common import fileutils
#from cinder.openstack.common import strutils
from oslo_utils import strutils
from cinder import utils
from cinder.volume import driver

LOG = logging.getLogger(__name__)

volume_opts = [
    cfg.StrOpt('zfs_zpool', default='cinder-volumes',
               help='Name for the zpool that will contain exported volumes'),
]

CONF = cfg.CONF
CONF.register_opts(volume_opts)


def _sizestr(size_in_g):
    if int(size_in_g) == 0:
        return '100m'
    return '%sg' % size_in_g


def _fromsizestr(sizestr):
    # ZFS uses a dash as a 'not applicable' value.
    if sizestr == '-':
        return None

    # ZFS tools give their sizes as 1T, 9.2G. string_to_bytes
    # doesn't accept that format, so we'll have to add a B.
    try:
        return strutils.string_to_bytes(sizestr + "B")
    except ValueError:
        return strutils.string_to_bytes(sizestr)


class ZFSVolumeDriver(driver.ISCSIDriver):
    """Executes commands relating to Volumes."""

    VERSION = '1.0.1'

    def __init__(self, *args, **kwargs):
        super(ZFSVolumeDriver, self).__init__(*args, **kwargs)
        self.hostname = socket.gethostname()
        self.zpool = self.configuration.zfs_zpool
        self.backend_name =\
            self.configuration.safe_get('volume_backend_name') or 'ZFS'

        target_driver = \
            self.target_mapping[self.configuration.safe_get('iscsi_helper')]

        LOG.debug('Attempting to initialize ZFS driver with the '
                  'following target_driver: %s',
                  target_driver)

        self.target_driver = importutils.import_object(
            target_driver,
            configuration=self.configuration,
            db=self.db,
            executor=self._execute)
        self.protocol = self.target_driver.protocol

    def check_for_setup_error(self):
        """Verify that requirements are in place to use ZFS driver."""

        # Call zpool, if the volume doesn't exist it will result in an error
        self._execute('zpool', 'status', self.zpool,
                      run_as_root=True)

    def create_volume(self, volume):
        """Creates a logical volume."""
        if not self.volume_exists(volume):
            self._execute('zfs', 'create',
                          '-s',  # sparse
                          '-b', '32K', # blocksize
                          '-V', _sizestr(volume['size']),
                          "%s/%s" % (self.zpool, volume['name']),
                          run_as_root=True)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""

        # FIXME: this will fail if the volume already exists, which may happen
        # during a retry cycle.

        src_path = "%s/%s@%s" % (self.zpool, snapshot['volume_name'],
                                 snapshot['name'])
        dst_path = "%s/%s" % (self.zpool, volume['name'])

        self._execute('zfs', 'clone', src_path, dst_path,
                      run_as_root=True)

    def _delete_volume(self, volume):
        """Deletes a logical volume."""

        full_name = "%s/%s" % (self.zpool, volume['name'])

        try:
            # Try to promote dependent volumes
            stdout, stderr = self._execute('zfs', 'get', '-H', '-r',
                                           '-t', 'snapshot',
                                           'clones', full_name,
                                           run_as_root=True)

            for line in stdout.splitlines():
                if not line:
                    continue

                clones = line.split('\t')[2]
                if clones and clones != '-':
                    for clone in clones.split(','):
                        self._execute('zfs', 'promote', clone,
                                      run_as_root=True)

            self._execute('zfs', 'destroy', '-r', full_name,
                          run_as_root=True)
            return True
        except processutils.ProcessExecutionError as err:
            if "dataset is busy" in err.stderr:
                mesg = ('Unable to delete volume %s due to it being busy' %
                        volume['name'])
                LOG.error(mesg)
                raise exception.VolumeIsBusy(volume_name=volume['name'])
            elif "dataset does not exist" not in err.stderr:
                mesg = ('Error reported running zfs destroy: '
                          'CMD: %(command)s, RESPONSE: %(response)s' %
                        {'command': err.cmd, 'response': err.stderr})
                LOG.error(mesg)
                raise

    def delete_volume(self, volume):
        for attempt in range(3):
            try:
                return self._delete_volume(volume)
            except exception.VolumeIsBusy:
                if attempt != 2:
                    time.sleep(15)
                else:
                    raise

    def create_snapshot(self, snapshot):
        """Creates a snapshot."""

        cmd = ['zfs', 'snapshot',
               "%s/%s@%s" % (self.zpool, snapshot['volume_name'],
                             snapshot['name'])]

        try:
            self._execute(*cmd,
                          run_as_root=True)
        except processutils.ProcessExecutionError as err:
            LOG.exception('Error creating snapshot')
            LOG.debug('Cmd     :%s' % err.cmd)
            LOG.debug('StdOut  :%s' % err.stdout)
            LOG.debug('StdErr  :%s' % err.stderr)
            raise

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot."""

        full_name = "%s/%s@%s" % (self.zpool, snapshot['volume_name'],
                                  snapshot['name'])

        try:
            self._execute('zfs', 'destroy', "-d", full_name,
                          run_as_root=True)
        except processutils.ProcessExecutionError as err:
            if "volume has children" in err.stderr:
                mesg = ('Unable to delete snapshot due to dependent '
                          'snapshots for volume: %s' % full_name)
                LOG.error(mesg)
                raise exception.VolumeIsBusy(
                    volume_name=snapshot['volume_name'])
            elif "dataset does not exist" not in err.stderr:
                mesg = ('Error reported running zfs destroy: '
                          'CMD: %(command)s, RESPONSE: %(response)s' %
                        {'command': err.cmd, 'response': err.stderr})
                LOG.error(mesg)
                raise

    def volume_exists(self, volume):
        full_name = "%s/%s" % (self.zpool, volume['name'])

        try:
            self._execute('zfs', 'list', full_name, run_as_root=True)
        except processutils.ProcessExecutionError as err:
            if 'dataset does not exist' in err.stderr:
                return False
            raise

        return True

    def snapshot_exists(self, snapshot):
        if self.volume_exists({'name': snapshot['volume_name']}):
            snapshots = self._list_snapshots({'name': snapshot['volume_name']})
            return any(s['name'] == snapshot['name'] for s in snapshots)
        return False

    def _list_snapshots(self, volume):
        full_name = "%s/%s" % (self.zpool, volume['name'])

        (out, err) = self._execute('zfs', 'list', '-t', 'snapshot', '-r', '-H',
                                   full_name, run_as_root=True)

        result = []
        for line in out.splitlines():
            name, used, available, referenced, mountpoint = line.split()
            result.append({
                'name': name.split('@')[-1],
                'volume_name': name.split('/', 1)[1],
                'zpool': name.split('/')[0],
                'used': _fromsizestr(used),
                'available': _fromsizestr(available),
                'referenced': _fromsizestr(referenced),
                'mountpoint': mountpoint,
            })

        return result

    def local_path(self, volume):
        return "/dev/zvol/%s/%s" % (self.zpool, volume['name'])

    @utils.synchronized('zfs-imgcache', external=False)
    def copy_image_to_volume(self, context, volume, image_service, image_id):
        """Fetch the image from image_service and write it to the volume."""

        img_cache_volume = {'name': 'img-%s' % image_id}
        img_cache_snapshot = {'volume_name': img_cache_volume['name'],
                              'name': 'snap'}

        if self._list_snapshots(volume):
            # If the volume has snapshots, there is no efficient way to replace
            # the contents. Do things the inefficient way.
            image_utils.fetch_to_raw(context, image_service, image_id,
                                     self.local_path(volume),
                                     self.configuration.volume_dd_blocksize,
                                     size=volume['size'])

        else:

            if not self.snapshot_exists(img_cache_snapshot):
                with image_utils.temporary_file() as tmp:
                    image_utils.fetch_verify_image(context, image_service,
                                                   image_id, tmp)

                    qemu_info = image_utils.qemu_img_info(tmp)
                    virtual_size_gb = math.ceil(
                        qemu_info.virtual_size / (1024.0 ** 3))
                    img_cache_volume['size'] = virtual_size_gb

                    self.create_volume(img_cache_volume)

                    image_utils.convert_image(
                        tmp, self.local_path(img_cache_volume), 'raw', None,
                        sparse=64 * 1024)

                self.create_snapshot(img_cache_snapshot)

            self.delete_volume(volume)
            self.create_volume_from_snapshot(volume, img_cache_snapshot)
            self.extend_volume(volume, volume['size'])

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        """Copy the volume to the specified image."""
        image_utils.upload_volume(context,
                                  image_service,
                                  image_meta,
                                  self.local_path(volume))

    def create_cloned_volume(self, volume, src_vref):
        """Creates a clone of the specified volume."""
        volume_name = src_vref['name']
        temp_snapshot = {'volume_name': volume_name,
                         'name': 'origin-%s' % volume['id']}

        self.create_snapshot(temp_snapshot)
        self.create_volume_from_snapshot(volume, temp_snapshot)

    @utils.synchronized('zfs-imgcache', external=False)

    def clone_image(self, context, volume,
                    image_location, image_meta,
                    image_service):
        img_cache_snapshot = {'volume_name': 'img-%s' % image_meta['id'],
                              'name': 'snap'}

        if self.snapshot_exists(img_cache_snapshot):
            self.create_volume_from_snapshot(volume, img_cache_snapshot)
            self.extend_volume(volume, volume['size'])
            return None, True

        return None, False

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume."""
        volume = self.db.volume_get(context, backup['volume_id'])
        volume_path = self.local_path(volume)
        with utils.temporary_chown(volume_path):
            with open(volume_path) as volume_file:
                backup_service.backup(backup, volume_file)

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""
        volume_path = self.local_path(volume)
        with utils.temporary_chown(volume_path):
            with open(volume_path, 'wb') as volume_file:
                backup_service.restore(backup, volume['id'], volume_file)

    def get_volume_stats(self, refresh=False):
        """Get volume status.

        If 'refresh' is True, run update the stats first.
        """

        if refresh:
            self._update_volume_stats()

        return self._stats

    def _update_volume_stats(self):
        """Retrieve stats info from volume group."""

        out, err = self._execute('zpool', 'list',
                                 '-H', "-oname,free,size",
                                 self.zpool, run_as_root=True)

        name, avail, size = out.strip().split('\t')

        data = {}

        # Note(zhiteng): These information are driver/backend specific,
        # each driver may define these values in its own config options
        # or fetch from driver specific configuration file.
        data["volume_backend_name"] = self.backend_name
        data["vendor_name"] = 'Open Source'
        data["driver_version"] = self.VERSION
        data["storage_protocol"] = "iSCSI"

        gb_factor = 1.0 / (1024 ** 3)
        data['total_capacity_gb'] = _fromsizestr(size) * gb_factor
        data['free_capacity_gb'] = _fromsizestr(avail) * gb_factor
        data['reserved_percentage'] = self.configuration.reserved_percentage
        data['QoS_support'] = False
        data['location_info'] = \
            'ZFSVolumeDriver:%s:%s' % (self.hostname, self.zpool)

        self._stats = data

    def extend_volume(self, volume, new_size):
        """Extend an existing voumes size."""

        try:
            self._execute('zfs', 'set', 'volsize=%s' % _sizestr(new_size),
                          '%s/%s' % (self.zpool, volume['name']),
                          run_as_root=True)
        except processutils.ProcessExecutionError as err:
            LOG.exception('Error extending Volume')
            LOG.debug('Cmd     :%s' % err.cmd)
            LOG.debug('StdOut  :%s' % err.stdout)
            LOG.debug('StdErr  :%s' % err.stderr)
            raise

        try:
            self._execute('gpart', 'commit', 'zvol/%s/%s' % (self.zpool, volume['name']))
        except processutils.ProcessExecutionError as err:
            LOG.debug('Cmd     :%s' % err.cmd)
            LOG.debug('StdOut  :%s' % err.stdout)
            LOG.debug('StdErr  :%s' % err.stderr)

    def ensure_export(self, context, volume):
        volume_name = volume['name']
        iscsi_name = "%s%s" % (self.configuration.iscsi_target_prefix,
                               volume_name)
        volume_path = self.local_path(volume)
        # NOTE(jdg): For TgtAdm case iscsi_name is the ONLY param we need
        # should clean this all up at some point in the future
        model_update = self.target_driver.ensure_export(context, volume,
                                                        volume_path)
        if model_update:
            self.db.volume_update(context, volume['id'], model_update)

    def create_export(self, context, volume, connector):
        """Creates an export for a logical volume."""

        volume_path = self.local_path(volume)

        data = self.target_driver.create_export(context, volume, volume_path)
        return {
            'provider_location': data['location'],
            'provider_auth': data['auth'],
        }

    def remove_export(self, context, volume):
        self.target_driver.remove_export(context, volume)
