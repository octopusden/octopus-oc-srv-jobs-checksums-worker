#!/usr/bin/env python3

from oc_checksumsq.checksums_interface import queue_published, FileLocation, ChecksumsQueueServer
import os
import urllib.parse
import argparse
import logging
from oc_orm_initializator.orm_initializator import OrmInitializator
from oc_cdtapi import NexusAPI
import tempfile

class LocationOverwriteError(Exception):
    def __init__(self, path):
        super().__init__("Location '%s' was physically overwritten but removing is disabled" % path)

class QueueWorkerApplication(ChecksumsQueueServer):

    def __init__(self, *args, **kvargs):
        self.setup_orm = kvargs.pop('setup_orm', True)
        self.controller = kvargs.pop('controller', None)
        self.remove = False
        super().__init__(*args, **kvargs)

    @property
    def __psql_schema(self):
        """
        Parse schema from PSQL_URL
        It should be set correctly
        """
        return urllib.parse.parse_qs(urllib.parse.urlparse(self.args.psql_url).query).get('search_path').pop(0)

    def ping(self):
        return

    def init(self, args):
        """
        Details initialization
        :param argparse.Namespace args: parsed command-line arguments
        """
        if args.remove == 'no':
            self.remove = False         # never remove artifacts from DB
        elif args.remove == 'always':
            self.remove = True    # always remove artifacts from DB if not found at repo
        else:
            self.remove = None

        logging.debug("Removing non-existing locations: %s" % str(self.remove))
        logging.info("PSQL_SCHEMA: %s" % self.__psql_schema)

        if not self.setup_orm:
            logging.debug("ORM initialization skipped")
            return

        _installed_apps = list()

        if not self.controller:
            if self.__psql_schema == 'dl_schema':
                _installed_apps.append("oc_delivery_apps.checksums")
            elif self.__psql_schema == 'cnt_schema':
                _installed_apps.append("oc_delivery_apps.dlcontents")

        logging.debug("ORM initialiation starting...")
        OrmInitializator(
            url=self.args.psql_url,
            user=self.args.psql_user,
            password=self.args.psql_password,
            installed_apps=_installed_apps)

        if self.controller:
            logging.debug("ORM initialization done, controller has been provided already")
            return

        if self.__psql_schema == 'dl_schema':
            from oc_delivery_apps.checksums import controllers
        elif self.__psql_schema == 'cnt_schema':
            from oc_delivery_apps.dlcontents import controllers

        self.controller = controllers.CheckSumsController()
        logging.debug("ORM initialization done, controller imported")

    def custom_args(self, parser):
        """
        Append specific arguments for this worker
        :param argparse.ArgumentParser parser: parser with arguments
        :return argparse.ArgumentParse: modified parser with additional arguments
        """
        parser.add_argument('--max-depth', help='Maximal registration depth', type=int, default=0)
        parser.add_argument('--remove', help='Remove artifact from database on 404 error - yes, no or always',
                            choices=['yes', 'no', 'always'], default='no')
        parser.add_argument("--psql-url", dest="psql_url", help="PSQL URL, including schema path",
                            default=os.getenv("PSQL_URL", "psql://localhost:5432/postgres?search_path=test_schema"))
        parser.add_argument("--psql-user", dest="psql_user", help="PSQL user",
                            default=os.getenv("PSQL_USER"))
        parser.add_argument("--psql-password", dest="psql_password", help="PSQL password",
                            default=os.getenv("PSQL_PASSWORD"))
        parser.add_argument("--mvn-url", dest="mvn_url", help="MVN URL",
                            default=os.getenv("MVN_URL", "http://localhost:8081/mvn"))
        parser.add_argument("--mvn-user", dest="mvn_user", help="MVN user",
                            default=os.getenv("MVN_USER"))
        parser.add_argument("--mvn-password", dest="mvn_password", help="MVN password",
                            default=os.getenv("MVN_PASSWORD"))

        return parser

    def prepare_parser(self):
        """
        Prepare command line arguments parser
        :return argparse.ArgumentParser: empty parser
        """
        return argparse.ArgumentParser(description='Checksums queue worker')

    def register_file(self, location, citype, depth=0, remove=False, version=None, client=None, parent=None, artifact_deliverable=None):
        """
        Registers file in database

        :param Tuple location: File Location Tuple (LOC_TYPE, Location, revision)
        :param str citype:  CITYPE code
        :param int depth:  archive registration depth
        :param bool remove:  if True or string - object will be deleted if not exists in repo
                             string should contain deletion reason for cases where caller deleted
                             this object on purpose
        :param str version: not used, added for compatibility with Mongo-based worker
        :param str client: not used, added for compatibility with Mongo-based worker
        :param str parent: not used, added for compatibility with Mongo-based worker
        :param bool artifact_deliverable: not used, added for compatibility with Mongo-based worker
        """

        _loc = FileLocation(*location)
        logging.info("Received registration request for location %s citype=%s depth=%d", str(_loc), citype, depth)

        if depth > self.args.max_depth:
            depth = self.args.max_depth
            logging.warning("Changed depth for %s to %d due to max-depth limitation of this worker", str(_loc), depth)

        reason = "Not exist in repo"

        if isinstance(remove, str):
            reason = remove

        if self.remove is not None:
            remove = self.remove

        self._register_location(_loc, citype, depth, remove=remove, reason=reason)

    def register_checksum(self, location, checksum, citype=None, cs_prov='Regular', mime='Data', cs_alg='MD5', version=None, client=None, parent=None, artifact_deliverable=None):
        """
        Register checksum for location if it is calculated already

        :param Tuple location: File Location tuple (LOC_TYPE, Location, revision)
        :param str checksum: checksum
        :param str citype:  CITYPE code
        :param str cs_prov: checksums provider
        :param str mime: MIME-type of a file
        :param str cs_alg: checksums algorithm
        :param str version: not used, added for compatibility with Mongo-based worker
        :param str client: not used, added for compatibility with Mongo-based worker
        :param str parent: not used, added for compatibility with Mongo-based worker
        :param bool artifact_deliverable: not used, added for compatibility with Mongo-based worker
        """
        logging.debug('Registering %s checksum %s', location, checksum)
        loc = FileLocation(*location)
        if cs_alg != 'MD5':
            raise NotImplementedError('MD5 checksum is supported only')

        self.controller.register_file_md5(checksum, citype, mime, loc.path, loc.loctype_code, loc.revision, cs_prov)

    def _register_location(self, location, citype=None, depth=0, remove=False, reason="Object does not exist"):
        """
        Location registration implementation
        :param FileLocation location: location tuple
        :param str citype: CI Type code
        :param int depth: depth of archive calculation
        :param bool remove: remove non-existent location
        :param str reason: reason of removing
        """
        if location.loctype_code != "NXS":
            raise NotImplementedError("Nexus archive registration is currently supported only")

        # MVN client is necessary for further actions
        _mvn = NexusAPI.NexusAPI(root=self.args.mvn_url,
                user=self.args.mvn_user,
                auth=self.args.mvn_password,
                readonly=False,
                anonymous=False)
        # first check if artifact has been removed
        logging.debug("Checking artifact exists: '%s'" % location.path)

        if not _mvn.exists(location.path):
            logging.debug("Not found in MVN: '%s'" % location.path)

            if remove:
                logging.info("Removing from DB: '%s'" % location.path)
                self.controller.delete_location(location.path, location.loctype_code, reason=reason)
            else:
                logging.info("Not found in MVN but not deleted: '%s'" % location.path)
            return

        logging.debug("Found in MVN: '%s'" % location.path)

        # further algoritm is:
        # 1. Take checksum from MVN - if available. Download locally if not.
        # 2. Get location checksum from database and compare to one we have got.
        # 3. Turn full registration if checksum differ. Return
        # 4 Compare depth level. Download and Register if not equal to one we are requested for. Return.
        _artifact_info = _mvn.info(location.path)
        _tempfile = None

        if not _artifact_info:
            logging.debug("No info from MVN for '%s'" % location.path)
            _artifact_info = dict()

        if not all([_artifact_info.get("md5"), _artifact_info.get("mime")]):
            _tempfile = self._download(_mvn, location.path)
            # we should raise an exception in case of failure so no using '.get' dict method
            logging.debug("Calculating MD5 and MIME from file downloaded")
            _artifact_info['md5'] = self.controller.md5(_tempfile)
            _artifact_info['mime'] = self.controller.mime(_tempfile)
            logging.debug("MD5 for '%s': '%s'" % (location.path, _artifact_info["md5"]))
            logging.debug("MIME for '%s': '%s'" % (location.path, _artifact_info["mime"]))


        # to get rid of possible temfile bugs (was in Python 2.7) we need to close _temfile directly in case of failure too
        try:
            if self._check_artifact_not_registered(location, depth, _artifact_info, remove=remove):
                _tempfile = self._register_artifact(_mvn, location, citype, depth, _artifact_info, _tempfile)
        finally:
            if _tempfile and not _tempfile.closed:
                logging.debug("Closing TempFile")
                _tempfile.close()

    def _download(self, mvn_client, gav):
        """
        Downloads artifact from MVN into temporary file and returns it
        :param NexusAPI mvn_client: active NexusAPI instance
        :param gav: gav to download
        :return tempfile.NamedTemporaryFile:
        """
        _result = tempfile.NamedTemporaryFile()
        logging.info("Downloading '%s' to '%s'" % (gav, _result.name))
        try:
            mvn_client.cat(gav, binary=True, stream=True, write_to=_result)
        except Exception as _e:
            # we have to close tempfile in case of failure due to 'tempfile' module feature
            _result.close()
            raise

        logging.debug("Downloaded '%s' to '%s'" % (gav, _result.name))
        return _result

    def _check_artifact_not_registered(self, location, depth, artifact_info, remove=False):
        """
        Check if artifact is to be registered or not.
        Delete the location given if one is registered already but checksums differ
        :param FileLocation loctaion: location tuple
        :param int depth: depth regi# disable extra logging output
        :param dict artifact_info: artifact information ("md5", "mime")
        :param bool remove: remove old location if overwrite detected
        :return bool: is registartion necessary or not
        """
        logging.debug("Checking artifact should be registered")
        _checksum_cur = self.controller.get_location_checksum(location.path, location.loctype_code)
        logging.debug("Current checksum in database: %s" % _checksum_cur)

        if not _checksum_cur:
            logging.debug("No checksum for '%s' in database, returning True" % location.path)
            return True

        if _checksum_cur != artifact_info.get("md5"):
            # locatin was overwritten!
            logging.info("Location '%s' was overwritten: '%s' <> '%s'" % (location.path, _checksum_cur, artifact_info.get("md5")))

            if not remove:
                raise LocationOverwriteError(location.path)

            logging.info("Removing old location checksum from DB: '%s'" % location.path)
            self.controller.delete_location(location.path, location.loctype_code, reason="Location was overwritten")
            logging.debug("Old '%s' location removed, returning True" % location.path)
            return True

        # checksums are equal, next verification is on depth
        logging.debug("Checksums equal, checking registration depth")
        _fl_cur = self.controller.get_file_by_location(location.path, location.loctype_code, history=False)

        if not _fl_cur:
            # this should never happen:
            logging.warning("No file by location found in DB, possible data inconsistence: '%s'" % location.path)
            return True

        _depth_cur = self.controller.get_current_inclusion_depth(_fl_cur) or 0
        logging.debug("Current depth is: %d" % _depth_cur)

        if _depth_cur < depth:
            logging.info("'%s' is registered with depth %d (needed %d), returning True" % (location.path, _depth_cur, depth))
            return True

        logging.debug("Checking finished, returning False")
        return False

    def _register_artifact(self, mvn_client, location, citype, depth, artifact_info, tmpfile=None):
        """
        Do registration depending on depth given
        :param NexusAPI mvn_client: active NexusAPI instance
        :param FileLocation loctaion: location tuple
        :param str citype: CI_TYPE code
        :param int depth: depth registration level (archive content)
        :param dict artifact_info: artifact information ("md5", "mime")
        :param temfile.NamedTemporaryFile: tempfile (if was downloaded previously)
        :return tempfile.NamedTemporaryFile: not None if one was downloaded anywhen
        """
        # all checks should be done before calling this routine
        logging.debug("Registering '%s' with depth %d" % (location.path, depth))

        if not depth:
            logging.debug("Registering checksum for '%s' since depth is zero" % location.path)
            self.controller.register_file_md5(artifact_info.get("md5"), citype, artifact_info.get("mime"), location.path, location.loctype_code)
            return tmpfile

        logging.debug("Registering '%s' with depth %d" % (location.path, depth))

        if not tmpfile:
            tmpfile = self._download(mvn_client, location.path)

        self.controller.register_file_obj(tmpfile, citype, location.path, location.loctype_code, inclusion_level=depth)

        return tmpfile

if __name__ == '__main__':
    exit(QueueWorkerApplication().main())
