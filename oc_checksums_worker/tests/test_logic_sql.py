from .helpers import archive_test_case
from .mocks import mock_sql_wrap
import os
from .generators.generators import generate_gav

class RegistrationBasicSqlTest(archive_test_case.ArchiveTestCase):
    def setUp(self):
        super().setUp()
        self.app.controller._sql_wrap = mock_sql_wrap.mock_sql_wrap

    def test_sql_basic(self):
        _sql_gav = "sql-base:delete_as_regular_id:0:sql"
        self.assertTrue(self.mvn.exists(_sql_gav))
        self.register(_sql_gav)
        # direct registration of "SQL" from "MVN" should give 1 checksum
        # since we do not store "SQL"s there (yet?)
        self.check_counters(Files=1, CheckSums=1, Locations=1, HistoricalLocations=1)

    def test_sql_basic_depth1(self):
        _sql_gav = "sql-base:delete_as_regular_id:0:sql"
        self.assertTrue(self.mvn.exists(_sql_gav))
        self.register(_sql_gav, depth=1)
        # here we should have full "SQL" workflow
        # NOTE: number of CheckSums may be different regarding on SQL
        self.check_counters(Files=1, CheckSums=6, Locations=1, HistoricalLocations=1)
        # check actual depth is zero
        _fl = archive_test_case.base_test_case.models.Files.objects.last()
        self.assertEqual(_fl.depth_level, 0)
        self.assertEqual(self.ck_controller.get_current_inclusion_depth_calc(_fl), 0)

    def test_plb_basic_depth1(self):
        _sql_gav = "sql-base:delete_as_regular_id:0:plb"
        self.assertTrue(self.mvn.exists(_sql_gav))
        self.register(_sql_gav, depth=1)
        # here we should have full "SQL" workflow
        self.check_counters(Files=1, CheckSums=4, Locations=1, HistoricalLocations=1)
        _fl = archive_test_case.base_test_case.models.Files.objects.last()
        self.assertEqual(_fl.depth_level, 0)
        self.assertEqual(self.ck_controller.get_current_inclusion_depth_calc(_fl), 0)

    def test_plb_over_sql_basic_depth1(self):
        _sql_gav = "sql-base:delete_as_regular_id:0:sql"
        _plb_gav = "sql-base:delete_as_regular_id:0:plb"
        self.assertTrue(self.mvn.exists(_sql_gav))
        self.register(_sql_gav, depth=1)
        self.register(_plb_gav, depth=1)
        # we have one actual file with two locations - one wrapped, one plain
        self.check_counters(Files=1, CheckSums=6, Locations=2, HistoricalLocations=2)
        _fl = archive_test_case.base_test_case.models.Files.objects.last()
        self.assertEqual(_fl.depth_level, 0)
        self.assertEqual(self.ck_controller.get_current_inclusion_depth_calc(_fl), 0)

    def test_sql_zip(self):
        # we have many prepared SQL/PLB samples
        # pack them all and register one zip
        _base_pth = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mocks", "sql-stubs")
        _all_sql_aids = list()

        for _dir in os.listdir(_base_pth):
            _fdir = os.path.join(_base_pth, _dir)

            if not os.path.isdir(_fdir):
                continue

            for _fn in os.listdir(_fdir):
                _f, _e = os.path.splitext(_fn)

                if not _e:
                    continue

                _e = _e.strip(".")

                _all_sql_aids.append((_f, _e))

        _all_sql_gavs = list(map(lambda x: "sql:%s:0:%s" % x, _all_sql_aids))
        _gav_zip = generate_gav("zip")
        self.mvn.create_gav(_gav_zip, include=_all_sql_gavs)
        self.register_check(_gav_zip, depth=1)
        # here we are ought to know specific of our SQL test samples
        # Files: 1 zip + 10 SQLs (each one is wrapped also)
        # but we have 2 sqls with comments only difference, so -1
        self.check_counters(Files=10)
        # CheckSums: 1 zip + 6*9 for SQLs + 2 for different form SQL = 57
        self.check_counters(CheckSums=57)
        # Locations: 1 zip + 10*2 for SQLs = 21
        self.check_counters(Locations=21, HistoricalLocations=21)

    def test_sql_morphing(self):
        # we have two samples with a little difference for this test exactly
        for _gav in ['sql-base:delete_as_regular_id:0:sql', 'sql-base:delete_as_regular_id_cmnt_chid:0:sql']:
            self.register(_gav, depth=1)

        # but they should not be treated as the same file because of different body (argument in function)
        self.check_counters(Files=2, CheckSums=12, Locations=2, HistoricalLocations=2)

    def test_sql_duplication(self):
        # send actually the same SQL from different locations
        # the difference is in comments only
        for _gav in ['sql-base:delete_as_regular_id:0:sql', 'sql:delete_as_regular_id_cmnt:0:sql']:
            self.register(_gav, depth=1)

        # but checksums will be 8 (not 5), because versions with comments should be different
        self.check_counters(Files=1, CheckSums=8, Locations=2, HistoricalLocations=2)

    def test_sql_duplication_history(self):
        # register two flat files and then re-register them as SQL
        # using special prepared ones
        for _gav in ['sql-base:delete_as_regular_id:0:sql', 'sql:delete_as_regular_id_cmnt:0:sql']:
            self.register(_gav)
            self.register(_gav, depth=1)

        self.check_counters(Files=2, CheckSums=8, Locations=2, HistoricalLocations=5)

        self.assertEqual(1, archive_test_case.base_test_case.models.Files.objects.filter(file_dup=None).count()) 
        _fl_r = archive_test_case.base_test_case.models.Files.objects.filter(file_dup=None).last()
        self.assertEqual(2, archive_test_case.base_test_case.models.Locations.objects.filter(file=_fl_r).count())
        self.assertEqual(1, archive_test_case.base_test_case.models.Files.objects.filter(file_dup=_fl_r).count())
        self.assertEqual(8, archive_test_case.base_test_case.models.CheckSums.objects.filter(file=_fl_r).count())
        # for real file should be 4 history records, and for second one (file_dup) - one only (add)
        self.assertEqual(4, 
                archive_test_case.base_test_case.models.HistoricalLocations.objects.filter(file=_fl_r).count())
