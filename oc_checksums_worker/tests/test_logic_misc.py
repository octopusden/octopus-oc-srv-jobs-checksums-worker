from .helpers import archive_test_case
import tempfile
from .generators.generators import generate_bytes, generate_gav, generate_many_different_gavs
import hashlib
import os

class RegistrationMiscTest(archive_test_case.ArchiveTestCase):

    def test_weird_data_got_registered(self):
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(generate_bytes())
        _tf.seek(0, os.SEEK_SET)
        _md5 = hashlib.md5(_tf.read()).hexdigest()
        _gavs = ["g:a:v1:zip",  "g:a:v2:zip"]
        
        for _gav in _gavs:
            self.mvn.set_gav_content(_gav, _tf) 
            self.ck_controller.register_file_obj(_tf, "FILE", _gav, "NXS")

        # just check our weird data is available 
        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[0], "NXS"),
                self.ck_controller.get_file_by_location(_gavs[1], "NXS"))

        for _gav in _gavs:
            self.assertEqual(1, archive_test_case.base_test_case.models.Locations.objects.filter(
                path=_gav, loc_type__code="NXS").count())

            self.assertEqual(_md5, self.ck_controller.get_location_checksum(_gav, "NXS"))

        self.assertNotEqual(
                archive_test_case.base_test_case.models.Locations.objects.get(path=_gavs[0], loc_type__code="NXS"),
                archive_test_case.base_test_case.models.Locations.objects.get(path=_gavs[1], loc_type__code="NXS"))

        # now send registration requests and check all the same
        for _gav in _gavs:
            self.register_check(_gav, depth=0)

        self.assertEqual(self.ck_controller.get_file_by_location(_gavs[0], "NXS"),
                self.ck_controller.get_file_by_location(_gavs[1], "NXS"))

        for _gav in _gavs:
            self.assertEqual(1, archive_test_case.base_test_case.models.Locations.objects.filter(
                path=_gav, loc_type__code="NXS").count())

            self.assertEqual(_md5, self.ck_controller.get_location_checksum(_gav, "NXS"))

        self.assertNotEqual(
                archive_test_case.base_test_case.models.Locations.objects.get(path=_gavs[0], loc_type__code="NXS"),
                archive_test_case.base_test_case.models.Locations.objects.get(path=_gavs[1], loc_type__code="NXS"))

    def test_zip_registration(self):
        # normal real zip registration
        _gav = 'gg:aa:vv:zip'
        self.mvn.create_gav(_gav, include=generate_many_different_gavs(2, p='txt'))
        self.register_check(_gav, depth=1)
        self.check_counters(Files=3, Locations=3, CheckSums=3, HistoricalLocations=3)

    def test_empty_zip_registration_check(self):
        # empty zip registration
        _gav = 'gg:aa:vv:zip'
        self.mvn.create_gav(_gav)
        self.register_check(_gav, depth=1)
        self.check_counters(Files=1, Locations=1, CheckSums=1, HistoricalLocations=1)

    def test_zip_that_hides(self):
        # zip is not a real zip
        _gav = 'gg:aa:vv:txt'
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(generate_bytes())
        self.mvn.set_gav_content(_gav, _tf)
        self.register(_gav, depth=1)
        # but actual registration should be with depth 0
        self.assertEqual(0, self.ck_controller.get_file_by_location(_gav, "NXS").depth_level)

    def test_two_archives_that_share_file(self):
        # two different files with one the same file but in different archive paths
        _gav_txt = "g:a:v1:txt"
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(generate_bytes())

        for _i in range(0, 2):
            self.mvn.set_gav_content(_gav_txt + ":%d" % (_i+1), _tf)

        _duplicates = list(map(lambda x: "%s:%d" %(_gav_txt, x), range(0,2)))

        for _i in range(0, 2):
            _gav = "g:a:v%d:zip" % (_i+1)
            _duplicates.append(_gav)
            _incls = generate_many_different_gavs(2, p='txt', current=[_gav_txt + ":%d" % (_i+1)],
                    duplicates=_duplicates)
            self.mvn.create_gav(_gav, include=_incls)
            self.register_check(_gav, depth=1)
            _duplicates += _incls

        # Files: two zips + two different in each zip + one the same: 2+2*2+1=7
        self.check_counters(Files=7, CheckSums=7)
        # Locations: two zips + three inslide each zip: 2+2*3=8
        self.check_counters(Locations=8, HistoricalLocations=8)
        # make sure we have exact checksum for the "shared" file
        self.assertEqual(self.mvn.info(_gav_txt + ':1').get("md5"), self.mvn.info(_gav_txt + ":2").get("md5"))
        _md5 = self.mvn.info(_gav_txt + ":1").get("md5")
        _fl = self.ck_controller.get_file_by_checksum(_md5)
        _locs = archive_test_case.base_test_case.models.Locations.objects.filter(file=_fl, loc_type__code="ARCH")
        self.assertEqual(2, _locs.count())
        self.assertNotEqual(_locs.first().file_dst, _locs.last().file_dst)
        self.assertNotEqual(_locs.first().path, _locs.last().path)

    def test_two_archives_that_share_file_with_same_name(self):
        # two different archive with same path included but with different content
        _gav_txt = "g:a:v:txt"
        self.mvn.create_gav(_gav_txt)

        _duplicates = list()

        for _i in range(0, 2):
            _gav = "g:a:v%d:zip" % (_i+1)
            _duplicates.append(_gav)
            _incls = generate_many_different_gavs(2, p='txt', current=[_gav_txt], duplicates=_duplicates)
            self.mvn.create_gav(_gav, include=_incls)
            self.register_check(_gav, depth=1)
            _duplicates += _incls

        # Files: two zips + two different in each zip + one the same: 2+2*2+1=7
        self.check_counters(Files=7, CheckSums=7)
        # Locations: two zips + two different inslide each zip + 1 the same, but twice since file_dst differs: 2+2*3=8
        self.check_counters(Locations=8, HistoricalLocations=8)
        _md5 = self.mvn.info(_gav_txt).get("md5")
        _fl = self.ck_controller.get_file_by_checksum(_md5)
        _locs = archive_test_case.base_test_case.models.Locations.objects.filter(file=_fl, loc_type__code="ARCH")
        self.assertEqual(2, _locs.count())
        self.assertNotEqual(_locs.first().file_dst, _locs.last().file_dst)
        self.assertEqual(_locs.first().path, _locs.last().path)

class RegistrationDepthTest(archive_test_case.ArchiveTestCase):
    # depth-specific tests

    def test_register_plain_file_nonzero_depth(self):
        _gav = generate_gav('txt')
        self.mvn.create_gav(_gav)
        self.register(_gav, depth=1)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        # check that real depth from database is 0    
        self.assertEqual(_loc.file.depth_level, 0)
        self.assertEqual(self.ck_controller.get_current_inclusion_depth_calc(_loc.file), 0)

    def test_register_zip_file_depth_0(self):
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(_gav, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        _md5 = self.mvn.info(_gav).get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.last()
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(0, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

    def test_register_zip_file_depth_1(self):
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(_gav, depth=1)
        self.check_counters(Files=4, CheckSums=4, Locations=4, HistoricalLocations=4)

        _md5 = self.mvn.info(_gav).get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(1, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

    def test_register_zip_file_depth_2(self):
        #NOTE: we have --max-depth=1 in base class, so should register with 1 depth regardless of our arg
        _gav_incl, _gav = generate_many_different_gavs(2, p='zip')
        _duplicates = [_gav_incl, _gav]
        _incls = generate_many_different_gavs(3, duplicates=_duplicates)
        self.mvn.create_gav(_gav_incl, include=_incls)
        _duplicates += _incls
        _incls = generate_many_different_gavs(3, current=[_gav_incl], duplicates=_duplicates)
        self.mvn.create_gav(_gav, include=_incls)
        self.register(_gav, depth=2)
        self.check_counters(Files=5, CheckSums=5, Locations=5, HistoricalLocations=5)

        _nfo = self.mvn.info(_gav)
        _md5 = _nfo.get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(1, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

        # check all includes
        for _k, _v in _nfo.get("include").items():
            # get_file_by_location does not give 'ARCH' correctly, take from models
            _loc_f = archive_test_case.base_test_case.models.Locations.objects.get(
                    file_dst=_loc.file, loc_type__code="ARCH", path=_k)
            self.assertIsNotNone(_loc_f)
            self.assertEqual(0, self.ck_controller.get_current_inclusion_depth_calc(_loc_f.file))
            self.assertEqual(self.ck_controller.get_checksum_by_file(_loc_f.file, "MD5"), _v.get("info").get("md5"))

    def test_register_zip_file_depth_3(self):
        #NOTE: we have --max-depth=1 in base class, so should register with 1 depth regardless of our arg
        _gav_ii, _gav_i, _gav = generate_many_different_gavs(3, p='zip')
        _duplicates = [ _gav_ii, _gav_i, _gav]
        _incls = generate_many_different_gavs(3, p='txt', duplicates=_duplicates)
        self.mvn.create_gav(_gav_ii, include=_incls)
        _duplicates += _incls
        _incls = generate_many_different_gavs(3, p='txt', current=[_gav_ii], duplicates=_duplicates)
        self.mvn.create_gav(_gav_i, include=_incls)
        _duplicates += _incls
        _incls = generate_many_different_gavs(3, p='txt', current=[_gav_i, _gav_ii], duplicates=_duplicates)
        self.mvn.create_gav(_gav, include=_incls)
        self.register(_gav, depth=3)
        self.check_counters(Files=6, CheckSums=6, Locations=6, HistoricalLocations=6)

        _nfo = self.mvn.info(_gav)
        _md5 = _nfo.get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(1, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

        # check all includes
        for _k, _v in _nfo.get("include").items():
            # get_file_by_location does not give 'ARCH' correctly, take from models
            _loc_f = archive_test_case.base_test_case.models.Locations.objects.get(
                    file_dst=_loc.file, loc_type__code="ARCH", path=_k)
            self.assertIsNotNone(_loc_f)
            self.assertEqual(0, self.ck_controller.get_current_inclusion_depth_calc(_loc_f.file))
            self.assertEqual(self.ck_controller.get_checksum_by_file(_loc_f.file, "MD5"), _v.get("info").get("md5"))

    def test_register_zip_file_with_depth_1_and_0(self):
        # check that real depth is not changed after all
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(_gav, depth=1)
        self.check_counters(Files=4, CheckSums=4, Locations=4, HistoricalLocations=4)

        _md5 = self.mvn.info(_gav).get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(1, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

        self.register(_gav, 0)
        # check nothing has changed
        self.check_counters(Files=4, CheckSums=4, Locations=4, HistoricalLocations=4)
        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(1, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

    def test_register_maxdepth(self):
        # reset max depth to zero and register real zip-archive
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=generate_many_different_gavs(3, p='txt'))
        self.register(_gav, depth=1, add_args=["--max-depth", "0"])
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        _md5 = self.mvn.info(_gav).get("md5")
        _tf = tempfile.NamedTemporaryFile()
        self.mvn.cat(_gav, write_to=_tf)
        _tf.seek(0, os.SEEK_SET)
        self.assertEqual(_md5, hashlib.md5(_tf.read()).hexdigest())
        _tf.close()

        _cs = archive_test_case.base_test_case.models.CheckSums.objects.get(checksum=_md5, cs_type__code="MD5")
        self.assertEqual(_md5, _cs.checksum)
        _loc = archive_test_case.base_test_case.models.Locations.objects.get(file=_cs.file, loc_type__code="NXS")
        self.assertEqual(_loc.path, _gav)
        self.assertEqual(self.ck_controller.get_file_by_checksum(_md5, "MD5"), _loc.file)
        self.assertEqual(0, self.ck_controller.get_current_inclusion_depth_calc(_loc.file))

