from . import base_test_case
import unittest.mock
from ..mocks import mvn_mock

class ArchiveTestCase(base_test_case.BaseTestCase):
    def setUp(self):
        super().setUp()
        base_test_case.models.LocTypes(code="ARCH", name="Inside Archive").save()
        base_test_case.models.CiTypes(code="STUFF", name="Something that matters").save()
        self.mvn = mvn_mock.MvnClientMock()

    def tearDown(self):
        self.mvn.clear()
        super().tearDown()

    def run_main(self, add_args):
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mmck.return_value = self.mvn
            super().run_main(add_args)
            _mmck.assert_called_once()

    def register(self, gav, citype=None, depth=0, add_args=None):
        location = base_test_case.FileLocation(gav, 'NXS', None)
        self.rpc.register_file(location, citype, depth)
        self.run_main(add_args)

    def register_check(self, gav, citype=None, depth=0, add_args=None):
        self.register(gav, citype, depth, add_args)
        _info = self.mvn.info(gav)
        _fr = self.ck_controller.get_file_by_location(gav, "NXS")
        self.assertIsNotNone(_fr)
        self.assertTrue(_fr.depth_level >= depth)
        _loc = base_test_case.models.Locations.objects.filter(file=_fr, loc_type__code="NXS", path=gav)
        self.assertEqual(_loc.count(), 1)
        _loc = _loc.last()
        self.assertEqual(_loc.file, _fr)
        self.assertIsNone(_loc.file_dst)
        self.assertEqual(base_test_case.models.CheckSums.objects.filter(
            file=_fr, cs_type__code="MD5", checksum=_info.get("md5")).count(), 1)

        if not _info.get("include") or not depth:
            # this is not ZIP archive, or it should be registered without includes
            # if current depth level is zero - check no "children" has been actually registered
            if not self.ck_controller.get_current_inclusion_depth_calc(_fr):
                self.assertEqual(0, 
                    base_test_case.models.Locations.objects.filter(
                        loc_type__code="ARCH", file_dst=_fr).count())
            return

        for _pth, _nfo in _info.get("include").items():
            _md5 = _nfo.get("info").get("md5")
            _loc = base_test_case.models.Locations.objects.filter(loc_type__code="ARCH", path=_pth, file_dst=_fr)
            self.assertEqual(_loc.count(), 1)
            _loc = _loc.last()
            self.assertEqual(base_test_case.models.CheckSums.objects.filter(
                file=_loc.file, cs_type__code="MD5", checksum=_md5).count(), 1)
