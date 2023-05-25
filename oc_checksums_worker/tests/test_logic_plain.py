from .helpers import archive_test_case
from .generators.generators import generate_gav, generate_many_different_gavs, generate_bytes
import tempfile

# do not be confused with 'ArchiveTestCase' - all its logic is suitable here
class RegistrationPlainTest(archive_test_case.ArchiveTestCase):
    def test_register_single(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

    def test_register_twice(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

    def test_register_single_empty(self):
        _gav = generate_gav()
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.set_gav_content(_gav, _tf)
        self.register_check(_gav)
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

    def test_register_double(self):
        # Register two unrelated files
        for _gav in generate_many_different_gavs(2):
            self.mvn.create_gav(_gav)
            self.register_check(_gav)
            self.register_check(_gav)
            _loc = archive_test_case.base_test_case.models.Locations.objects.last()
            self.assertEqual(_loc.path, _gav)
            self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

        self.check_counters(Files=2, CheckSums=2, Locations=2, HistoricalLocations=2)

    def test_register_change(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

        while True:
            self.mvn.create_gav(_gav)

            if _md5 != self.mvn.info(_gav).get("md5"):
                break

        self.register_check(_gav)
        self.check_counters(Files=2, CheckSums=2, Locations=1, HistoricalLocations=3)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

    def test_register_change_to_emptiness(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)
        self.mvn.set_gav_content(_gav, tempfile.NamedTemporaryFile())
        self.assertNotEqual(_md5, self.mvn.info(_gav).get("md5"))
        self.register_check(_gav)
        self.check_counters(Files=2, CheckSums=2, Locations=1, HistoricalLocations=3)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), 
                self.mvn.info(_gav).get("md5"))

    def test_register_file_copy(self):
        _gavs = generate_many_different_gavs(2)
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(generate_bytes())

        _md5_g = None
        for _gav in _gavs:
            self.mvn.set_gav_content(_gav, _tf)
            _md5 = self.mvn.info(_gav).get("md5")

            if not _md5_g:
                _md5_g = _md5
            else:
                self.assertEqual(_md5_g, _md5)

            self.register_check(_gav)
            self.check_counters(Files=1, CheckSums=1)
            self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)

        self.check_counters(Files=1, CheckSums=1, Locations=2, HistoricalLocations=2)

    def test_unregister_file(self):
        _gav = generate_gav()
        self.mvn.create_gav(_gav)
        _md5 = self.mvn.info(_gav).get("md5")
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_location_checksum(_gav, "NXS"), _md5)
        self.mvn.clear()
        self.register(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=0, HistoricalLocations=2)
        self.assertEqual(archive_test_case.base_test_case.models.Locations.objects.count(), 0)
        self.assertIsNone(self.ck_controller.get_location_checksum(_gav, "NXS"))
        _f = self.ck_controller.get_file_by_location(_gav, "NXS", history=True)
        self.assertIsNotNone(_f)
        self.assertEqual(self.ck_controller.get_checksum_by_file(_f), _md5)
