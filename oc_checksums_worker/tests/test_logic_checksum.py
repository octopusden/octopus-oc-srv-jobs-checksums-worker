from .helpers import base_test_case
from oc_checksumsq.checksums_interface import FileLocation
from .generators.generators import generate_bytes
import hashlib
from oc_delivery_apps.checksums import models

class RegisterChecksumTest(base_test_case.BaseTestCase):

    def __ckregister(self, path, p_type, rev, sample=None, reg_args={}):
        loc = FileLocation(path, p_type, rev)

        if sample is None:
            sample = generate_bytes()

        _md5 = hashlib.md5(sample).hexdigest()
        self.rpc.register_checksum(loc, _md5, **reg_args)
        self.run_main()
        return (loc, _md5)

    def __ckregverify(self, path, p_type, rev, reg_args={}):
        (_loc, _md5) = self.__ckregister(path, p_type, rev)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)
        self.assertEqual(self.ck_controller.get_location_checksum(path, p_type, revision=rev), _md5)
        _fl = models.Files.objects.last() # it is OK since we have verified that this is the only object in DB
        _loc = models.Locations.objects.last() # the same as above
        self.assertEqual(_loc.file.pk, _fl.pk)
        self.assertEqual(_loc.loc_type.code, p_type)
        self.assertEqual(_loc.path, path)
        _cs = models.CheckSums.objects.get(cs_type__code='MD5', checksum=_md5)
        self.assertEqual(_cs.file.pk, _fl.pk)

    def test_cksum_register_smb(self):
        self.__ckregverify('smb:///host/dir/file', 'SMB', None)

    def test_cksum_register_smb_full(self):
        self.__ckregverify('smb:///host/dir/file1', 'SMB', None, 
                            reg_args={'citype': 'OTHER', 'cs_prov': 'Regular', 'mime': 'data', 'cs_alg': 'MD5'})

    def test_cksum_register_nxs_full(self):
        self.__ckregverify('g:aaa:v1.1:pkg', 'NXS', None,
                            reg_args={'citype': 'OTHER', 'cs_prov': 'Regular', 'mime': 'data', 'cs_alg': 'MD5'})
    
    def test_cksum_register_svn_full(self):
        self.__ckregverify('https://vcs-svn.example.com/svn/branches/branch/path/to/file.bin', 'SVN', 37,
                            reg_args={'citype': 'OTHER', 'cs_prov': 'Regular', 'mime': 'data', 'cs_alg': 'MD5'})

    def test_cksum_register_svn_no_rev(self):
        # here file is to be registered only, location will raise ValueError with revision required
        self.__ckregister('https://vcs-svn.example.com/svn/branches/branch/path/to/file.bin', 'SVN', None)
        self.check_counters(Files=1, Locations=0, CheckSums=1, HistoricalLocations=0)

    def test_cksum_added(self):
        _data = generate_bytes()
        self.__ckregister('smb:///dir/file', 'SMB', None, sample=_data)
        self.__ckregister('g:aaa123:v1.1:pkg', 'NXS', None, sample=_data)
        self.__ckregister('https://vcs-svn.example.com/svn/branches/branch/path/to/file.bin', 'SVN', 337, sample=_data)
        self.__ckregister('https://vcs-svn.example.com/svn/branches/branch/path/othfile.bin', 'SVN', None, sample=_data)
        # last one have to be skipped due to revision missing, 3 locations should be only
        self.check_counters(Files=1, CheckSums=1, Locations=3, HistoricalLocations=3)




