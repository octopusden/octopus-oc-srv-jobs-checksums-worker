import tempfile
from .helpers import archive_test_case
from .generators.generators import generate_gav, generate_bytes, generate_many_different_gavs

class RegistrationGenTest(archive_test_case.ArchiveTestCase):

    def test_register_single(self):
        _gav_zip = generate_gav('zip')
        self.mvn.create_gav(_gav_zip, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(gav=_gav_zip, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

    def test_register_twice_same_depth(self):
        _gav_zip = generate_gav('zip')
        self.mvn.create_gav(_gav_zip, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(gav=_gav_zip, depth=1)
        self.check_counters(Files=4, CheckSums=4, Locations=4, HistoricalLocations=4)

        self.register_check(gav=_gav_zip, depth=0)
        self.check_counters(Files=4, CheckSums=4, Locations=4, HistoricalLocations=4)

    def test_register_twice_zero_depth(self):
        _gav_zip = generate_gav('zip')
        self.mvn.create_gav(_gav_zip, include=generate_many_different_gavs(3, p='txt'))
        self.register_check(gav=_gav_zip, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        self.register_check(gav=_gav_zip, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

    def test_register_single_empty(self):
        _gav_txt = generate_gav('txt')
        self.mvn.create_gav(_gav_txt)
        self.mvn.set_gav_content(_gav_txt, tempfile.NamedTemporaryFile())
        # NOTE: registration without check since file is empty so depth should be zero actually
        self.register(gav=_gav_txt, depth=1)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_file_by_location(_gav_txt, "NXS").depth_level, 0)

    def test_register_double(self):
        # test register two unrelated files
        # make sure we have two different files in MVN
        _gavs = generate_many_different_gavs(3, p='txt')

        for _gav in _gavs:
            self.mvn.create_gav(_gav)
            self.register_check(_gav)

        self.check_counters(Files=len(_gavs), CheckSums=len(_gavs), Locations=len(_gavs), HistoricalLocations=len(_gavs))

    def test_register_change(self):
        # test register one file overwrite
        _gavs = generate_many_different_gavs(3, p='txt')

        for _gav in _gavs:
            self.mvn.create_gav(_gav)
            self.register_check(_gav)

        self.check_counters(Files=len(_gavs), CheckSums=len(_gavs), Locations=len(_gavs), HistoricalLocations=len(_gavs))

        # now overwrite one of gavs and register all of them once again
        self.mvn.create_gav(_gavs[0])
        while len(set(filter(lambda y: bool(y), 
            list(map(lambda x: self.mvn.info(x).get("md5"), _gavs))))) != len(_gavs):
                self.mvn.create_gav(_gavs[0])

        for _gav in _gavs:
            self.register_check(_gav)

        # writing check_counters like this give more obvious picture on asserts
        self.check_counters(Files=len(_gavs) + 1)
        self.check_counters(CheckSums=len(_gavs) + 1)
        self.check_counters(Locations=len(_gavs))
        # "plus two" since location is to be deleted first
        self.check_counters(HistoricalLocations=len(_gavs) + 2)

    def test_register_change_to_emptiness(self):
        #test register one file overwrite to empty"
        _gav_txt = generate_gav('txt')
        self.mvn.create_gav(_gav_txt)
        self.register_check(_gav_txt)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        _oldfile = self.ck_controller.get_file_by_location(_gav_txt, "NXS")
        self.mvn.set_gav_content(_gav_txt, tempfile.NamedTemporaryFile())
        self.register_check(_gav_txt)
        self.check_counters(Files=2, CheckSums=2, Locations=1, HistoricalLocations=3)
        _newfile = self.ck_controller.get_file_by_location(_gav_txt, "NXS")
        self.assertNotEqual(_oldfile, _newfile)
        self.assertNotEqual(self.ck_controller.get_checksum_by_file(_oldfile),
                self.ck_controller.get_checksum_by_file(_newfile))
       
    def test_register_file_copy(self):
        _gavs = generate_many_different_gavs(2)
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(generate_bytes())

        for _gav in _gavs:
            self.mvn.set_gav_content(_gav, _tf)

        self.assertEqual(self.mvn.info(_gavs[0]).get('md5'), self.mvn.info(_gavs[1]).get('md5'))

        #test register one file twice with different paths
        for _gav in _gavs:
            self.register_check(_gav)

        self.check_counters(Files=1, CheckSums=1, Locations=2, HistoricalLocations=2)
        _fl = self.ck_controller.get_file_by_location(_gavs[0], "NXS", history=False)
        self.assertEqual(_fl, self.ck_controller.get_file_by_location(_gavs[1], "NXS", history=False))
        # check all our locations are referring to one file
        self.assertEqual(archive_test_case.base_test_case.models.Locations.objects.filter(file=_fl).count(), 2)
        # and our historical locations are referring to different location records

        for _lr in archive_test_case.base_test_case.models.Locations.objects.filter(file=_fl):
            self.assertEqual(_lr.history.count(), 1)


    def test_register_file_nested(self):
        #test register file and an archive containing file
        _gav_txt = generate_gav('txt')
        _gav_zip = generate_gav('zip')
        self.mvn.create_gav(_gav_txt)
        self.mvn.create_gav(_gav_zip, include=[_gav_txt])
        self.register_check(gav=_gav_txt, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        # now include this file into archive
        self.register_check(gav=_gav_zip, depth=0)
        self.check_counters(Files=2, CheckSums=2, Locations=2, HistoricalLocations=2)

        self.register_check(gav=_gav_zip, depth=1)
        self.check_counters(Files=2, CheckSums=2, Locations=3, HistoricalLocations=3)

        # additional asserts:
        # assert two files are really different
        # assert checksums are for two different files
        _fls = set(map(lambda x: self.ck_controller.get_file_by_location(x, "NXS"), [_gav_txt, _gav_zip]))
        self.assertEqual(len(_fls), 2)
        # assert checksums are for two different files
        self.assertEqual(2, len(set(map(lambda x: self.ck_controller.get_checksum_by_file(x), _fls))))
        # assert locations are for two different files
        # second one should have TWO locations
        self.assertEqual(1, archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="NXS", path=_gav_zip).count())
        _ltxt = archive_test_case.base_test_case.models.Locations.objects.filter(
            loc_type__code="NXS", path=_gav_txt)
        self.assertEqual(1, _ltxt.count())
        self.assertEqual(2, archive_test_case.base_test_case.models.Locations.objects.filter(
            file=_ltxt.last().file).count())
        _ltxt = archive_test_case.base_test_case.models.Locations.objects.filter(
            file=_ltxt.last().file, loc_type__code='ARCH')
        self.assertEqual(_ltxt.count(), 1)

    def test_unregister_file(self):
        _gav = generate_gav('txt')
        self.mvn.create_gav(_gav)
        self.register_check(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        # now kill this in MVN
        self.mvn.set_gav_content(_gav, None)
        self.register(_gav)
        self.check_counters(Files=1, CheckSums=1, Locations=0, HistoricalLocations=2)

    def test_unregister_file_current_depth(self):
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=[generate_gav('txt')])
        self.register_check(_gav, depth=1)
        self.check_counters(Files=2, CheckSums=2, Locations=2, HistoricalLocations=2)

        # now kill this in MVN
        self.mvn.set_gav_content(_gav, None)
        self.register(_gav, depth=1)
        self.check_counters(Files=2, CheckSums=2, Locations=1, HistoricalLocations=3)

    def test_unregister_file_increased_depth(self):
        _gav = generate_gav('zip')
        self.mvn.create_gav(_gav, include=[generate_gav('txt')])
        self.register_check(_gav, depth=0)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

        # now kill this in MVN
        self.mvn.set_gav_content(_gav, None)
        self.register(_gav, depth=1)
        self.check_counters(Files=1, CheckSums=1, Locations=0, HistoricalLocations=2)
