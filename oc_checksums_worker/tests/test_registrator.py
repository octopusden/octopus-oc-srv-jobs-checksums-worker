from .helpers import base_test_case
from .generators.generators import generate_gav
import unittest.mock
from .mocks import mvn_mock
from oc_checksumsq.checksums_interface import FileLocation
import tempfile
import os

class FileRegistrationTestSuite(base_test_case.BaseTestCase):
 
    def setUp(self):
        super().setUp()
        base_test_case.models.LocTypes(code="AT_TEST", name="AT_TEST").save()
        base_test_case.models.CiTypes(code="TEST", name="TEST").save()
        base_test_case.models.LocTypes(code="ARCH", name="Inside Archive").save()
        self.mvn = mvn_mock.MvnClientMock()

    def tearDown(self):
        self.mvn.clear()
        super().tearDown()

    def __run_register_location(self, *args, **kwargs):
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mmck.return_value = self.mvn
            self.app._register_location(*args, **kwargs)

    def __make_file_location(self, path):
        return FileLocation(path, "NXS", None)
 
    def test_existing_archive_required(self):
        _test_location = generate_gav()
        self.assertFalse(self.mvn.exists(_test_location))
        self.__run_register_location(self.__make_file_location(_test_location), "TEST")
        self.check_counters(Files=0, CheckSums=0, Locations=0, HistoricalLocations=0)
 
    def test_existing_archive_processed(self):
        _test_location = generate_gav()
        self.mvn.create_gav(_test_location)
        self.__run_register_location(self.__make_file_location(_test_location), "TEST")
        self.check_counters(Files=1, Locations=1, CheckSums=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_test_location, "NXS"), self.mvn.info(_test_location).get("md5"))
       
    def test_controller_error_reraised(self):
        # raise registration error
        self.ck_controller.register_file_md5 = unittest.mock.MagicMock(side_effect=ValueError("Registration failed"))
        self.ck_controller.register_file_obj = unittest.mock.MagicMock(side_effect=ValueError("Registration failed"))
        _test_location = generate_gav()
        self.mvn.create_gav(_test_location)

        with self.assertRaises(ValueError):
            self.__run_register_location(self.__make_file_location(_test_location), "TEST")

        self.check_counters(Files=0, Locations=0, CheckSums=0, HistoricalLocations=0)
 
    def test_archive_contents_processed(self):
        _gav_zip = generate_gav('zip')
        self.mvn.create_gav(_gav_zip, include=[generate_gav("txt")])
        self.__run_register_location(self.__make_file_location(_gav_zip), "TEST", depth=2)
        self.check_counters(Files=2, Locations=2, CheckSums=2, HistoricalLocations=2)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav_zip, "NXS"), self.mvn.info(_gav_zip).get("md5"))

        for _k, _v in self.mvn.info(_gav_zip).get("include").items():
            _md5 = _v.get("info").get("md5")
            _fl = self.ck_controller.get_file_by_checksum(_md5)
            self.assertIsNotNone(_fl)
            self.assertEqual(1, base_test_case.models.Locations.objects.filter(
                path=_k, loc_type__code="ARCH", file=_fl, file_dst=self.ck_controller.get_file_by_location(_gav_zip, "NXS")).count())


class RegistrationNecessityCheckTestSuite(base_test_case.BaseTestCase):

    def __make_file_location(self, path):
        return FileLocation(path, "NXS", None)

    def test_missing_md5_causes_registration(self):
        self.assertTrue(self.app._check_artifact_not_registered(self.__make_file_location(generate_gav()), 1, {}))

    def test_unknown_checksums_causes_registration(self):
        self.assertTrue(self.app._check_artifact_not_registered(self.__make_file_location(generate_gav()), 1, {"md5": "XXX"}))

    def test_insufficient_existing_depth_causes_registration(self):
        base_test_case.models.CiTypes(code="TEST", name="TEST").save()
        base_test_case.models.LocTypes(code="ARCH", name="Inside Archive").save()
        mvn = mvn_mock.MvnClientMock()
        _gav_zip = generate_gav("zip")
        mvn.create_gav(_gav_zip, include=[generate_gav("txt")])
        _tf = tempfile.NamedTemporaryFile()
        mvn.cat(_gav_zip, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.ck_controller.register_file_obj(_tf, "TEST", loc_path=_gav_zip, loc_type="NXS", inclusion_level=0)
        self.assertTrue(self.app._check_artifact_not_registered(self.__make_file_location(_gav_zip), 1, mvn.info(_gav_zip)))
        _tf.close()
        mvn.clear()

    def test_existing_registration_reused(self):
        base_test_case.models.CiTypes(code="TEST", name="TEST").save()
        base_test_case.models.LocTypes(code="ARCH", name="Inside Archive").save()
        mvn = mvn_mock.MvnClientMock()
        _gav_zip = generate_gav("zip")
        mvn.create_gav(_gav_zip, include=[generate_gav("txt")])
        _tf = tempfile.NamedTemporaryFile()
        mvn.cat(_gav_zip, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.ck_controller.register_file_obj(_tf, "TEST", loc_path=_gav_zip, loc_type="NXS", inclusion_level=0)
        self.assertFalse(self.app._check_artifact_not_registered(self.__make_file_location(_gav_zip), 0, mvn.info(_gav_zip)))
        _tf.close()
        mvn.clear() 
