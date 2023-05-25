import tempfile
from .helpers import archive_test_case
from .generators.generators import generate_many_different_gavs, generate_gav

class RegistrationCiTypesTest(archive_test_case.ArchiveTestCase):
    def _test_citype_zip_unchanged(self, register_ci_type='STUFF', expected_ci_type='STUFF'):
        _gav_txt = 'test:file:0.1:txt'
        _gav_zip = 'test:archive:0.1:zip'
        self.mvn.create_gav(_gav_txt)
        self.register_check(gav=_gav_txt, citype=register_ci_type, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav_txt, "NXS").ci_type.code, expected_ci_type)

        # lets drop some stuff in archive now!
        # ci_type shuld not be changed to 'FILE' while registering an archive
        self.mvn.create_gav(_gav_zip, include=generate_many_different_gavs(1, p='txt', current=['test:file:0.1:txt']))
        self.register_check(gav=_gav_zip, depth=1)
        self.check_counters(Files=3, CheckSums=3, Locations=4, HistoricalLocations=4)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav_txt, "NXS").ci_type.code, expected_ci_type)

    def test_citype_zip_unchanged__good(self):
        return self._test_citype_zip_unchanged()

    def test_citype_zip_unchanged__empty(self):
        return self._test_citype_zip_unchanged(register_ci_type="", expected_ci_type='FILE')

    def test_citype_zip_unchanged__none(self):
        return self._test_citype_zip_unchanged(register_ci_type=None, expected_ci_type='FILE')

    def test_citype_zip_unchanged__awful(self):
        # here we provide non-existent type
        # first-time registration should fail, and second one only should succeed
        _gav_txt = 'test:file:0.1:txt'
        _gav_zip = 'test:archive:0.1:zip'
        self.mvn.create_gav(_gav_txt)
        self.register(gav=_gav_txt, citype="NONEXIST", depth=0)
        self.check_counters(Files=0, CheckSums=0, Locations=0, HistoricalLocations=0)
        self.assertIsNone(self.ck_controller.get_file_by_location(_gav_txt, "NXS"))

        # now pack this into an archive, ci_type shuld not be set to 'FILE' while registering an archive
        # note the count of locations to check - one lower than previous
        self.mvn.create_gav(_gav_zip, include=generate_many_different_gavs(1, p='txt', current=['test:file:0.1:txt']))
        self.register_check(gav=_gav_zip, depth=1)
        self.check_counters(Files=3, CheckSums=3, Locations=3, HistoricalLocations=3)
        self.assertEqual(
                self.ck_controller.get_file_by_checksum(self.mvn.info(_gav_txt).get("md5")).ci_type.code, "FILE")

    def test_citype_zip_changed(self):
        # register archive with 'FILE' type
        # second one provide real type, it should be left the same
        # this may be changed in the future
        _gav = 'test:archive:0.1:zip'
        self.mvn.create_gav(_gav, include=list([generate_gav('txt'), generate_gav('bin')]))
        self.register_check(_gav, depth=1)
        self.check_counters(Files=3, CheckSums=3, Locations=3, HistoricalLocations=3)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'FILE')

        self.register_check(_gav, citype="STUFF")
        self.check_counters(Files=3, CheckSums=3, Locations=3, HistoricalLocations=3)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'FILE')

    def test_citype_same_location_changed(self):
        # register 3 different files with same location
        # ci_types should be different, number of files changed, location should be overwritten
        _gav = 'test:file:0.1:txt'
        self.mvn.create_gav(_gav)
        self.register_check(_gav, citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'STUFF')

        self.mvn.create_gav(_gav)
        self.register_check(_gav, citype=None)
        self.check_counters(Files=2, CheckSums=2, Locations=1, HistoricalLocations=3)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'FILE')

        self.mvn.create_gav(_gav)
        self.register_check(_gav, citype="OTHER")
        self.check_counters(Files=3, CheckSums=3, Locations=1, HistoricalLocations=5)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'OTHER')

    def test_citype_same_location_unchanged(self):
        # the same as above, but contents not changed
        _gav = 'test:file:0.1:txt'
        self.mvn.create_gav(_gav)
        self.register_check(_gav, citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'STUFF')

        self.register_check(_gav, citype=None)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'STUFF')

        self.register_check(_gav, citype="OTHER")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav, "NXS").ci_type.code, 'STUFF')

    def test_citype_changed(self):
        # some different locations with different ci_types but content is strictly the same
        _gavs = list("test:file:0.%d:txt" % (_d + 1) for _d in range(0,3))
        self.mvn.create_gav(_gavs[0])
        _md5 = self.mvn.info(_gavs[0]).get("md5")

        for _d in range(1, 3):
            _tf = tempfile.NamedTemporaryFile()
            self.mvn.cat(_gavs[0], write_to=_tf)
            _tf.flush()
            self.mvn.set_gav_content(_gavs[_d], _tf)
            self.assertEqual(_md5, self.mvn.info(_gavs[_d]).get("md5"))

        self.register_check(_gavs[0], citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[0], "NXS").ci_type.code, 'STUFF')

        self.register_check(_gavs[1], citype=None)
        self.check_counters(Files=1, CheckSums=1, Locations=2, HistoricalLocations=2)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[1], "NXS").ci_type.code, 'STUFF')

        self.register_check(_gavs[2], citype="OTHER")
        self.check_counters(Files=1, CheckSums=1, Locations=3, HistoricalLocations=3)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[2], "NXS").ci_type.code, 'STUFF')

    def test_citype_unchanged(self):
        # some different locations with same ci_types and content is strictly the same
        _gavs = list("test:file:0.%d:txt" % (_d + 1) for _d in range(0,3))
        self.mvn.create_gav(_gavs[0])
        _md5 = self.mvn.info(_gavs[0]).get("md5")

        for _d in range(1, 3):
            _tf = tempfile.NamedTemporaryFile()
            self.mvn.cat(_gavs[0], write_to=_tf)
            _tf.flush()
            self.mvn.set_gav_content(_gavs[_d], _tf)
            self.assertEqual(_md5, self.mvn.info(_gavs[_d]).get("md5"))

        self.register_check(_gavs[0], citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[0], "NXS").ci_type.code, 'STUFF')

        self.register_check(_gavs[1], citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=2, HistoricalLocations=2)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[1], "NXS").ci_type.code, 'STUFF')

        self.register_check(_gavs[2], citype="STUFF")
        self.check_counters(Files=1, CheckSums=1, Locations=3, HistoricalLocations=3)
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[2], "NXS").ci_type.code, 'STUFF')

    def test_citype_register_incorrect(self):
        _gavs = list("test:file:0.%d:txt" % (_d + 1) for _d in range(0,3))
        self.mvn.create_gav(_gavs[0])
        _md5 = self.mvn.info(_gavs[0]).get("md5")

        for _d in range(1, 3):
            _tf = tempfile.NamedTemporaryFile()
            self.mvn.cat(_gavs[0], write_to=_tf)
            _tf.flush()
            self.mvn.set_gav_content(_gavs[_d], _tf)
            self.assertEqual(_md5, self.mvn.info(_gavs[_d]).get("md5"))

        for _d in range(0, 5):
            _gav = "test:otherfile:0.0.%d:txt" % _d
            self.mvn.create_gav(_gav)
            _gavs.append(_gav)

        for _gav in _gavs:
            self.register(_gav, citype="AWFUL")
            self.check_counters(Files=0, CheckSums=0, Locations=0, HistoricalLocations=0)
            self.assertIsNone(self.ck_controller.get_file_by_location(_gav, "NXS"))


