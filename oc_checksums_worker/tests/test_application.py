import unittest
import unittest.mock

from oc_cdt_queue2.test.synchron.mocks.queue_loopback import LoopbackConnection, global_messaging, global_message_queue
from .mocks.checksums_worker import QueueWorkerApplicationMock
from oc_checksumsq.checksums_interface import ChecksumsQueueClient, FileLocation
import tempfile
from ..checksums_worker import LocationOverwriteError

# disable extra logging output
import logging
logging.getLogger().propagate = False
logging.getLogger().disabled = True

class QueueWorkerTest(unittest.TestCase):

    def setUp(self):
        self.app = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        self.rpc = ChecksumsQueueClient()

        self.rpc._Connection = LoopbackConnection
        self.app._Connection = LoopbackConnection

        self.rpc.setup('amqp://127.0.0.1/', queue='my')
        self.rpc.connect()

        self.app.max_sleep = 0

    def test_messages(self):
        self.assertTrue(global_messaging)
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my'])
        self.assertTrue('my' in global_message_queue)

        self.rpc.ping()
        self.app.main(['--queue', 'my', '--declare', 'yes', '--amqp-url', 'amqp://127.0.0.1/'])
        self.assertEqual(self.app.counter_messages, 1)
        self.assertEqual(self.app.counter_good, 1)
        self.assertEqual(self.app.counter_bad, 0)

        myfile = FileLocation('g:a:v:p', 'NXS', None)
        self.rpc.register_file(myfile, 'OTHER', 1)
        self.app.register_file = unittest.mock.MagicMock(return_value=None)
        self.app.main(['--queue', 'my', '--declare', 'yes', '--amqp-url', 'amqp://127.0.0.1/'])
        self.assertEqual(self.app.counter_messages, 2)
        self.assertEqual(self.app.counter_good, 2)
        self.assertEqual(self.app.counter_bad, 0)

        myfile = FileLocation('g:a:v:p1', 'NXS', None)
        self.rpc.register_file(myfile, 'OTHER', 1)
        self.app.register_file.side_effect=Exception("Registration Failed")
        self.app.main(['--queue', 'my', '--declare', 'yes', '--amqp-url', 'amqp://127.0.0.1/'])
        self.assertEqual(self.app.counter_messages, 3)
        self.assertEqual(self.app.counter_good, 2)
        self.assertEqual(self.app.counter_bad, 1)

    def test_maxdepth(self):
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my'])
        self.assertEqual(self.app.args.max_depth, 0)
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my', '--max-depth', '1'])
        self.assertEqual(self.app.args.max_depth, 1)

    def test_remove(self):
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my'])
        self.assertEqual(self.app.remove, False)
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my', '--remove', 'yes'])
        self.assertEqual(self.app.remove, None)
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my', '--remove', 'always'])
        self.assertEqual(self.app.remove, True)
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my', '--remove', 'no'])
        self.assertEqual(self.app.remove, False)


class WorkerLogicTest(unittest.TestCase):
    def test_init__remove(self):
        # revove value test
        _app = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _args = unittest.mock.MagicMock()
        _args.psql_url="amqp://localhost:5672?search_path=test_schema"
        _args.remove = 'no'
        _app.args = _args
        _app.init(_args)
        self.assertFalse(_app.remove)
        _args.remove = 'yes'
        _app.init(_args)
        self.assertIsNone(_app.remove)
        _args.remove = 'always'
        _app.init(_args)
        self.assertTrue(_app.remove)

    def test_init__orm_false(self):
        # test setup ORM - with and without controller
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _args = unittest.mock.MagicMock()
        _args.remove = 'no'
        _args.psql_url = "amqp://localhost:5672?search_path=test_schema"
        _args.psql_url = "amqp://localhost:5672?search_path=test_schema"
        _args.psql_user = "test_user"
        _args.psql_password = "test_password"
        _wrk.args = _args
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.OrmInitializator") as _x:
            _wrk.init(_args)
            _x.assert_not_called()

    def test_init__orm_true(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=True, controller=unittest.mock.MagicMock())
        _args = unittest.mock.MagicMock()
        _args.remove = 'no'
        _args.psql_url = "amqp://localhost:5672?search_path=test_schema"
        _args.psql_user = "test_user"
        _args.psql_password = "test_password"
        _wrk.args = _args
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.OrmInitializator") as _x:
            _wrk.init(_args)
            _x.assert_called_once_with(
                    url=_args.psql_url,
                    user=_args.psql_user,
                    password=_args.psql_password,
                    installed_apps=list())

    def test_custom_args(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _prs = unittest.mock.MagicMock()
        _prs.add_argument = unittest.mock.MagicMock()
        _prs = _wrk.custom_args(_prs)
        _prs.add_argument.assert_any_call('--max-depth', help='Maximal registration depth', type=int, default=0)
        _prs.add_argument.assert_any_call('--remove', help='Remove artifact from database on 404 error - yes, no or always',
                            choices=['yes', 'no', 'always'], default='no')
        _prs.add_argument.assert_any_call("--psql-url", dest="psql_url", help="PSQL URL, including schema path",
                            default="psql://localhost:5432/postgres?search_path=test_schema")
        _prs.add_argument.assert_any_call("--psql-user", dest="psql_user", help="PSQL user", default=None)
        _prs.add_argument.assert_any_call("--psql-password", dest="psql_password", help="PSQL password", default=None)
        _prs.add_argument.assert_any_call("--mvn-url", dest="mvn_url", help="MVN URL", default="http://localhost:8081/mvn")
        _prs.add_argument.assert_any_call("--mvn-user", dest="mvn_user", help="MVN user", default=None)
        _prs.add_argument.assert_any_call("--mvn-password", dest="mvn_password", help="MVN password", default=None)
        self.assertEqual(8, _prs.add_argument.call_count)        

    def test_register_file(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk._register_location = unittest.mock.MagicMock()
        _loc = ("The Path", "LCT", None)
        _wrk.remove = False
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.max_depth = 5
        _wrk.register_file(_loc, "CITYPE", 12, "The Remove Reason")
        _wrk._register_location.assert_called_once_with(FileLocation(*_loc), "CITYPE", 5, remove=False, reason="The Remove Reason")
        _wrk._register_location.reset_mock()
        _loc = ("AnotherPath", "ALCK", 1232)
        _wrk.remove = True
        _wrk.register_file(_loc, "ACTYPE", 3)
        _wrk._register_location.assert_called_once_with(FileLocation(*_loc), "ACTYPE", 3, remove=True, reason="Not exist in repo")

    def test_register_checksum(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())

        with self.assertRaises(NotImplementedError):
            _wrk.register_checksum(("Path", "LTC", None), "abcd", cs_alg="PUPZ")

        _wrk.register_checksum(("AnotherPath", "ALTC", None), "abcd", citype="PPP")
        _wrk.controller.register_file_md5.assert_called_once_with("abcd", "PPP", "Data", "AnotherPath", "ALTC", None, "Regular")
        _wrk.controller.register_file_md5.reset_mock()
        _wrk.register_checksum(("OtherPath", "OLTC", 11), "abcdef", "PRST", "Wrapped", "text/unknown", "MD5", "1.1.1", "CLT", "ParEnT", False)
        _wrk.controller.register_file_md5.assert_called_once_with("abcdef", "PRST", "text/unknown", "OtherPath", "OLTC", 11, "Wrapped")

    def test_register_location__not_supported(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        
        with self.assertRaises(NotImplementedError):
            _wrk._register_location(FileLocation("Path", "LC", None), "CTYPE")

    def test_register_location__not_exist_no_remove(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=False)
            _mmck.return_value=_mvn
            _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE")
            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()

    def test_register_location__not_exist_remove(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=False)
            _mmck.return_value=_mvn
            _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=True)
            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_called_once_with("G:A:V:P", "NXS", reason="Object does not exist")

    def test_register_location__no_info(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _tf = tempfile.NamedTemporaryFile()
            _tf.write(b"the_test_data")
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=True)
            _mvn.info = unittest.mock.MagicMock(return_value=None)
            _wrk.controller.md5 = unittest.mock.MagicMock(return_value="abcdef")
            _wrk.controller.mime = unittest.mock.MagicMock(return_value="unknown/unknown")
            _wrk._download = unittest.mock.MagicMock(return_value=_tf)
            _wrk._check_artifact_not_registered = unittest.mock.MagicMock(return_value=False)
            _wrk._register_artifact = unittest.mock.MagicMock()
            _mmck.return_value=_mvn
            _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=False)
            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()
            _mvn.info.assert_called_once_with("G:A:V:P")
            _wrk._download.assert_called_once_with(_mvn, "G:A:V:P")
            _wrk.controller.md5.assert_called_once_with(_tf)
            _wrk.controller.mime.assert_called_once_with(_tf)
            _wrk._check_artifact_not_registered.assert_called_once_with(FileLocation('G:A:V:P', 'NXS', None), 0, {'md5': 'abcdef', 'mime': 'unknown/unknown'}, remove=False)
            _wrk._register_artifact.assert_not_called()
            _tf.close()

    def test_register_location__artifact_not_registered(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=True)
            _mvn.info = unittest.mock.MagicMock(return_value={'md5': 'abcdef1234', 'mime': 'test/unknown'})
            _wrk.controller.md5 = unittest.mock.MagicMock()
            _wrk.controller.mime = unittest.mock.MagicMock()
            _wrk._download = unittest.mock.MagicMock()
            _wrk._check_artifact_not_registered = unittest.mock.MagicMock(return_value=True)
            _wrk._register_artifact = unittest.mock.MagicMock()
            _mmck.return_value=_mvn
            _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=False)
            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()
            _mvn.info.assert_called_once_with("G:A:V:P")
            _wrk._download.assert_not_called()
            _wrk.controller.md5.assert_not_called()
            _wrk.controller.mime.assert_not_called()
            _wrk._check_artifact_not_registered.assert_called_once_with(FileLocation('G:A:V:P', 'NXS', None), 0, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, remove=False)
            _wrk._register_artifact.assert_called_once_with(_mvn, FileLocation('G:A:V:P', 'NXS', None), 'CTYPE', 0, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, None)

    def test_register_location__artifact_registered(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=True)
            _mvn.info = unittest.mock.MagicMock(return_value={'md5': 'abcdef1234', 'mime': 'test/unknown'})
            _wrk.controller.md5 = unittest.mock.MagicMock()
            _wrk.controller.mime = unittest.mock.MagicMock()
            _wrk._download = unittest.mock.MagicMock()
            _wrk._check_artifact_not_registered = unittest.mock.MagicMock(return_value=False)
            _wrk._register_artifact = unittest.mock.MagicMock()
            _mmck.return_value=_mvn
            _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=False, depth=1)
            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()
            _mvn.info.assert_called_once_with("G:A:V:P")
            _wrk._download.assert_not_called()
            _wrk.controller.md5.assert_not_called()
            _wrk.controller.mime.assert_not_called()
            _wrk._check_artifact_not_registered.assert_called_once_with(FileLocation('G:A:V:P', 'NXS', None), 1, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, remove=False)
            _wrk._register_artifact.assert_not_called()

    def test_register_location__exception_verify(self):
        class VerificationException(Exception):
            pass

        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=True)
            _mvn.info = unittest.mock.MagicMock(return_value={'md5': 'abcdef1234', 'mime': 'test/unknown'})
            _wrk.controller.md5 = unittest.mock.MagicMock()
            _wrk.controller.mime = unittest.mock.MagicMock()
            _wrk._download = unittest.mock.MagicMock()
            _wrk._check_artifact_not_registered = unittest.mock.MagicMock(side_effect=VerificationException)
            _wrk._register_artifact = unittest.mock.MagicMock()
            _mmck.return_value=_mvn

            with self.assertRaises(VerificationException):
                _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=False)

            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()
            _mvn.info.assert_called_once_with("G:A:V:P")
            _wrk._download.assert_not_called()
            _wrk.controller.md5.assert_not_called()
            _wrk.controller.mime.assert_not_called()
            _wrk._check_artifact_not_registered.assert_called_once_with(FileLocation('G:A:V:P', 'NXS', None), 0, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, remove=False)
            _wrk._register_artifact.assert_not_called()

    def test_register_location__exception_registration(self):
        class RegistrationException(Exception):
            pass

        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk.args = unittest.mock.MagicMock()
        _wrk.args.mvn_url = "http://test.example.com/something"
        _wrk.args.mvn_user = "test_yser"
        _wrk.args.mvn_password = "test_xassword"
        
        with unittest.mock.patch("oc_checksums_worker.checksums_worker.NexusAPI.NexusAPI") as _mmck:
            _mvn = unittest.mock.MagicMock()
            _mvn.exists = unittest.mock.MagicMock(return_value=True)
            _mvn.info = unittest.mock.MagicMock(return_value={'md5': 'abcdef1234', 'mime': 'test/unknown'})
            _wrk.controller.md5 = unittest.mock.MagicMock()
            _wrk.controller.mime = unittest.mock.MagicMock()
            _wrk._download = unittest.mock.MagicMock()
            _wrk._check_artifact_not_registered = unittest.mock.MagicMock(return_value=True)
            _wrk._register_artifact = unittest.mock.MagicMock(side_effect=RegistrationException)
            _mmck.return_value=_mvn

            with self.assertRaises(RegistrationException):
                _wrk._register_location(FileLocation("G:A:V:P", "NXS", None), "CTYPE", remove=False, depth=3)

            _mmck.assert_called_once_with(root=_wrk.args.mvn_url, user=_wrk.args.mvn_user, auth=_wrk.args.mvn_password, readonly=False, anonymous=False)
            _mvn.exists.assert_called_once_with("G:A:V:P")
            _wrk.controller.delete_location.assert_not_called()
            _mvn.info.assert_called_once_with("G:A:V:P")
            _wrk._download.assert_not_called()
            _wrk.controller.md5.assert_not_called()
            _wrk.controller.mime.assert_not_called()
            _wrk._check_artifact_not_registered.assert_called_once_with(FileLocation('G:A:V:P', 'NXS', None), 3, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, remove=False)
            _wrk._register_artifact.assert_called_once_with(_mvn, FileLocation('G:A:V:P', 'NXS', None), 'CTYPE', 3, {'md5': 'abcdef1234', 'mime': 'test/unknown'}, None)

    def test_download(self):
        class AnyTempFile():
            def __eq__(self, other):
                if hasattr(other, "closed") and not other.closed:
                    other.close()
                return True

        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _mvn = unittest.mock.MagicMock()
        _wrk._download(_mvn, "G:A:V:P")
        _mvn.cat.assert_called_once_with("G:A:V:P", binary=True, stream=True, write_to=AnyTempFile())

    def test_check_artifact_not_registered__no_checksum(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value=None)
        _ctrl.get_file_by_location = unittest.mock.MagicMock()
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock()
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        self.assertTrue(_wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info))
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_not_called()
        _ctrl.get_file_by_location.assert_not_called()
        _ctrl.get_current_inclusion_depth.assert_not_called()

    def test_check_artifact_not_registered__other_checksum_no_remove(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value="abcdef")
        _ctrl.get_file_by_location = unittest.mock.MagicMock()
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock()
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        with self.assertRaises(LocationOverwriteError):
            _wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info)
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_not_called()
        _ctrl.get_file_by_location.assert_not_called()
        _ctrl.get_current_inclusion_depth.assert_not_called()

    def test_check_artifact_not_registered__other_checksum_remove(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value="abcdef")
        _ctrl.get_file_by_location = unittest.mock.MagicMock()
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock()
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        self.assertTrue(_wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info, remove=True))
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_called_once_with("g:a:v:pps", "NXS", reason="Location was overwritten")
        _ctrl.get_file_by_location.assert_not_called()
        _ctrl.get_current_inclusion_depth.assert_not_called()

    def test_check_artifact_not_registered__not_in_base(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value=_info.get("md5"))
        _ctrl.get_file_by_location = unittest.mock.MagicMock(return_value=None)
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock()
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        self.assertTrue(_wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info))
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_not_called()
        _ctrl.get_file_by_location.assert_called_once_with("g:a:v:pps", "NXS", history=False)
        _ctrl.get_current_inclusion_depth.assert_not_called()

    def test_check_artifact_not_registered__depth_not_equal(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value=_info.get("md5"))
        _ctrl.get_file_by_location = unittest.mock.MagicMock(return_value="anyfile")
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock(return_value=0)
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        self.assertTrue(_wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info))
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_not_called()
        _ctrl.get_file_by_location.assert_called_once_with("g:a:v:pps", "NXS", history=False)
        _ctrl.get_current_inclusion_depth.assert_called_once_with("anyfile")

    def test_check_artifact_not_registered__registered(self):
        _ctrl = unittest.mock.MagicMock()
        # file data
        _info = {"md5": "abcdef0123", "mime": "testdata/testcase"}
        # controller mocks
        _ctrl.get_location_checksum = unittest.mock.MagicMock(return_value=_info.get("md5"))
        _ctrl.get_file_by_location = unittest.mock.MagicMock(return_value="anyfile")
        _ctrl.get_current_inclusion_depth = unittest.mock.MagicMock(return_value=2)
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=_ctrl)
        self.assertFalse(_wrk._check_artifact_not_registered(FileLocation("g:a:v:pps", "NXS", None), 1, _info))
        _ctrl.get_location_checksum.assert_called_once_with("g:a:v:pps", "NXS")
        _ctrl.delete_location.assert_not_called()
        _ctrl.get_file_by_location.assert_called_once_with("g:a:v:pps", "NXS", history=False)
        _ctrl.get_current_inclusion_depth.assert_called_once_with("anyfile")

    def test_register_artifact__zero_depth(self):
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk._download = unittest.mock.MagicMock()
        _mvn = unittest.mock.MagicMock()
        _wrk._register_artifact(_mvn, FileLocation("groupId:artifactId:version:packaging", "NXS", None), "CITYPE", 0, {"md5": "abcd", "mime": "unknown/data"})
        _wrk._download.assert_not_called()
        _wrk.controller.register_file_obj.assert_not_called()
        _wrk.controller.register_file_md5.assert_called_once_with("abcd", "CITYPE", "unknown/data", "groupId:artifactId:version:packaging", "NXS")

    def test_register_artifact__no_tempfile(self):
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(b"TestData")
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk._download = unittest.mock.MagicMock(return_value=_tf)
        _mvn = unittest.mock.MagicMock()
        _wrk._register_artifact(_mvn, FileLocation("groupId:artifactId:version:packaging", "NXS", None), "CITYPE", 1, {"md5": "abcd", "mime": "unknown/data"})
        _wrk._download.assert_called_once_with(_mvn, "groupId:artifactId:version:packaging")
        _wrk.controller.register_file_md5.assert_not_called()
        _wrk.controller.register_file_obj.assert_called_once_with(_tf, "CITYPE", "groupId:artifactId:version:packaging", "NXS", inclusion_level=1)
        _tf.close()

    def test_register_artifact__with_tempfile_depth(self):
        _tf = tempfile.NamedTemporaryFile()
        _tf.write(b"TestDataFile")
        _wrk = QueueWorkerApplicationMock(setup_orm=False, controller=unittest.mock.MagicMock())
        _wrk._download = unittest.mock.MagicMock()
        _mvn = unittest.mock.MagicMock()
        _wrk._register_artifact(_mvn, FileLocation("groupId:artifactId:version:packaging", "NXS", None), "CITYPE", 1, {"md5": "abcd", "mime": "unknown/data"}, _tf)
        _wrk._download.assert_not_called()
        _wrk.controller.register_file_md5.assert_not_called()
        _wrk.controller.register_file_obj.assert_called_once_with(_tf, "CITYPE", "groupId:artifactId:version:packaging", "NXS", inclusion_level=1)
        _tf.close()
