# Copyright 2015 Chelsio Communications Inc.
# All Rights Reserved.
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


import os
import re

from oslo_concurrency import processutils as putils
from oslo_log import log as logging
from oslo_utils import netutils

from cinder import exception
from cinder.i18n import _LI, _LW, _LE
from cinder import utils
from cinder.volume.targets import iscsi
from cinder.volume import utils as vutils

LOG = logging.getLogger(__name__)


class CtlAdm(iscsi.ISCSITarget):
    """iSCSI target administration using ctladm."""

    def __init__(self, *args, **kwargs):
        super(CtlAdm, self).__init__(*args, **kwargs)
        self.ctld = self.configuration.safe_get('ctl_deamon')
        self.ctl_conf = self.configuration.safe_get('ctl_conf')
        self.iscsi_iotype = self.configuration.safe_get('ctl_backend')
	LOG.warn("ctl_conf: %s", self.configuration)

    def _is_block(self, path):
        mode = os.stat(path).st_mode
        return stat.S_ISBLK(mode)

    def _iotype(self, path):
         return 'block'


    def _get_iscsi_target(self, context, vol_id):
        return 0

    def _get_target_and_lun(self, context, volume):
        lun = 0
        iscsi_target = 0
        return iscsi_target, lun

    def _get_target_chap_auth(self, context, iscsi_name):
        """Get the current chap auth username and password."""
        return None

    def _get_target(self, iqn):
        conf_file = self.ctl_conf

        if os.path.exists(conf_file):
            with utils.temporary_chown(conf_file):
                try:
                    ctl_conf_text = open(conf_file, 'r+')
                    full_txt = ctl_conf_text.readlines()
                    for line in full_txt:
                        if re.search(iqn, line):
                            return iqn
                finally:
                    return none
        return None

    def _iscsi_authentication(self, chap, name, password):
        return ""

    def show_target(self, tid, iqn=None, **kwargs):

        conf_file = self.ctl_conf

        if iqn is None:
            raise exception.InvalidParameterValue(
                err=_('valid iqn needed for show_target'))

        if os.path.exists(conf_file):
            with utils.temporary_chown(conf_file):
                try:
                    ctl_conf_text = open(conf_file, 'r+')
                    full_txt = ctl_conf_text.readlines()
                    for line in full_txt:
                        if re.search(iqn, line):
                            ctl_conf_text.close()
                            return
                finally:
                    ctl_conf_text.close()

        raise exception.NotFound()

    @utils.synchronized('iscsi-ctl', external=False)
    def create_iscsi_target(self, name, tid, lun, path,
                            chap_auth=None, **kwargs):

        # NOTE (jdg): Address bug: 1175207
        kwargs.pop('old_name', None)

	"""tid = "foobar"""
	""" tid = name
	LOG.warn("tid: %s", tid)
	LOG.warn("conf_file: %s", conf_file) """
        conf_file = self.ctl_conf

        if os.path.exists(conf_file):
            with utils.temporary_chown(conf_file):
                try:
                    ctl_conf_text = open(conf_file, 'r+')
                    full_txt = ctl_conf_text.readlines()
                    for line in full_txt:
                        if re.search(name, line):
                            LOG.warn('%s already in config. Will not add again' % name)
                            ctl_conf_text.close()
                            return tid
                finally:
                    ctl_conf_text.close()


            try:
                volume_conf = """
target %s {
    auth-group no-authentication
    portal-group pg0
    lun 0 {
        backend %s
        path %s
    }
}
""" % (name, self._iotype(path), path)

                with utils.temporary_chown(conf_file):
                    f = open(conf_file, 'a+')
                    f.write(volume_conf)
                    f.close()
            except putils.ProcessExecutionError as e:
                vol_id = name.split(':')[1]
                LOG.error("Failed to create iscsi target for volume "
                            "id:%(vol_id)s: %(e)s"
                          % {'vol_id': vol_id, 'e': e})
                raise exception.ISCSITargetCreateFailed(volume_id=vol_id)

            utils.execute(self.ctld, 'onereload', run_as_root=True, log_errors=putils.LOG_ALL_ERRORS)

	""" LOG.warn("tid2: %s", tid)"""
        return tid

    def create_export(self, context, volume, volume_path):
        """Creates an export for a logical volume."""
        # 'iscsi_name': 'iqn.2010-10.org.openstack:volume-00000001'
        iscsi_name = "%s%s" % (self.configuration.iscsi_target_prefix,
                               volume['name'])
        iscsi_target, lun = self._get_target_and_lun(context, volume)

        # Verify we haven't setup a CHAP creds file already
        # if DNE no big deal, we'll just create it
        chap_auth = self._get_target_chap_auth(context, iscsi_name)
        if not chap_auth:
            chap_auth = (vutils.generate_username(),
                         vutils.generate_password())

        # NOTE(jdg): For TgtAdm case iscsi_name is the ONLY param we need
        # should clean this all up at some point in the future
        tid = self.create_iscsi_target(iscsi_name,
                                       iscsi_target,
                                       lun,
                                       volume_path,
                                       chap_auth)
        data = {}
        data['location'] = self._iscsi_location(
            self.configuration.iscsi_ip_address, tid, iscsi_name, lun,
            self.configuration.iscsi_secondary_ip_addresses)
        LOG.debug('Set provider_location to: %s', data['location'])
        data['auth'] = self._iscsi_authentication(
            'CHAP', *chap_auth)
        return data

    @utils.synchronized('iscsi-ctl', external=False)
    def remove_iscsi_target(self, tid, lun, vol_id, vol_name, **kwargs):
        LOG.info('Removing iscsi_target for volume: %s' % vol_id)
        vol_uuid_file = vol_name
        conf_file = self.ctl_conf
        if os.path.exists(conf_file):
            with utils.temporary_chown(conf_file):
                try:
                    ctl_conf_text = open(conf_file, 'r+')
                    full_txt = ctl_conf_text.readlines()
                    new_ctl_conf_txt = []
                    count = 0
                    for line in full_txt:
                        if count > 0:
                            count -= 1
                            continue
                        elif re.search(vol_uuid_file, line):
                            count = 7
                            continue
                        else:
                            new_ctl_conf_txt.append(line)

                    ctl_conf_text.seek(0)
                    ctl_conf_text.truncate(0)
                    ctl_conf_text.writelines(new_ctl_conf_txt)
                finally:
                    ctl_conf_text.close()
            utils.execute(self.ctld, 'onereload', run_as_root=True)
