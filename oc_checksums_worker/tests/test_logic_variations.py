from .helpers import archive_test_case
from .generators.generators import generate_gav, generate_many_different_gavs
import tempfile
import unittest.mock


class RegistrationPlainBrokenMD5Test(archive_test_case.ArchiveTestCase):
    #Run RegistrationPlainTest against NexusAPI that can't calculate MD5

    def test_unregister_file__exists_broken(self):
        # register one real file
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

        self.mvn.exists = unittest.mock.MagicMock(side_effect=Exception("Broken"))
        self.register(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

    def test_unregister_file__info_broken(self):
        # register one real file
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

        self.mvn.info = unittest.mock.MagicMock(side_effect=Exception("Broken"))
        self.register(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

    def test_unregister_file__cat_broken(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

        self.mvn.info = unittest.mock.MagicMock(return_value=dict())
        self.mvn.cat = unittest.mock.MagicMock(side_effect=Exception("Broken"))
        self.register(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

class RegistrationGenZipWithEmptyFilesTest(archive_test_case.ArchiveTestCase):
    # Run tests on zips that contains empty files
    def setUp(self):
        super().setUp()
        self._empty_gavs = generate_many_different_gavs(3, p='bin')
        self._used_gavs = list(self._empty_gavs)

        for _gav in self._empty_gavs:
            self.mvn.set_gav_content(_gav, tempfile.NamedTemporaryFile())

        self._zip_gavs = generate_many_different_gavs(2, p="zip", duplicates=self._empty_gavs)

        self._used_gavs += self._zip_gavs

        for _zip_gav in self._zip_gavs:
            _include = generate_many_different_gavs(3, p="txt", current=self._empty_gavs, duplicates=self._used_gavs)
            self.mvn.create_gav(_zip_gav, include=_include)
            self._used_gavs += _include

    def test_register_change(self):
        for _gav in self._zip_gavs:
            self.register(_gav, depth=1)

        # files= 2 zips + 2*3 different + 3 empty as 1 = 9
        # TODO: make this auto-calculated, not hardcoded
        self.check_counters(Files=8, CheckSums=8, Locations=8, HistoricalLocations=8)

        _md5 = None

        for _gav in self._empty_gavs:
            if not _md5:
                _md5 = self.mvn.info(_gav).get("md5")
            else:
                self.assertEqual(_md5, self.mvn.info(_gav).get("md5"))

        self.assertEqual(0, archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="ARCH", file=self.ck_controller.get_file_by_checksum(_md5)).count())

        # remove empty files from zips and register them second time

        for _zip_gav in self._zip_gavs:
            _include = generate_many_different_gavs(3, p="txt", duplicates=self._used_gavs)
            self.mvn.create_gav(_zip_gav, include=_include)
            self._used_gavs += _include

        for _gav in self._zip_gavs:
            self.register_check(_gav, depth=1)

        # plus 2 zips 3 gavs each, but 2 locations removed (historical appended with removing)
        # TODO: make this auto-calculated, not hardcoded
        self.check_counters(Files=16, CheckSums=16, Locations=14, HistoricalLocations=18)
        self.assertEqual(0, archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="ARCH", file=self.ck_controller.get_file_by_checksum(_md5)).count())

    
    def test_register_double(self):
        for _gav in self._zip_gavs:
            self.register(_gav, depth=1)
            # check will fail since empty files are SKIPPED at the moment

        # files= 2 zips + 2*3 different + 3 empty as 0 = 8
        # TODO: make this auto-calculated, not hardcoded
        self.check_counters(Files=8, CheckSums=8, Locations=8, HistoricalLocations=8)
        _md5 = None

        for _gav in self._empty_gavs:
            if not _md5:
                _md5 = self.mvn.info(_gav).get("md5")
            else:
                self.assertEqual(_md5, self.mvn.info(_gav).get("md5"))

        self.assertEqual(0, archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="ARCH", file=self.ck_controller.get_file_by_checksum(_md5)).count())

        # do the same and check nothing has been changed
        for _gav in self._zip_gavs:
            self.register(_gav, depth=1)

        self.check_counters(Files=8, CheckSums=8, Locations=8, HistoricalLocations=8)
        self.assertEqual(0, archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="ARCH", file=self.ck_controller.get_file_by_checksum(_md5)).count())

