import django.test
from . import django_settings
from oc_cdt_queue2.test.synchron.mocks.queue_loopback import LoopbackConnection
from oc_cdt_queue2.test.synchron.mocks.queue_loopback import global_messaging, global_message_queue, global_message_id
from oc_delivery_apps.checksums import models, controllers
from ..mocks.checksums_worker import QueueWorkerApplicationMock
from oc_checksumsq.checksums_interface import ChecksumsQueueClient, FileLocation

# disable extra logging output
import logging
logging.getLogger().propagate = False
logging.getLogger().disabled = True

class BaseTestCase(django.test.TransactionTestCase):
    def setUp(self):
        django.core.management.call_command('migrate', verbosity=0, interactive=False)
        global_message_queue.clear()
        global_message_id.clear()
        # check database is empty
        for _m in [models.LocTypes, models.CiTypes, models.Files, models.CheckSums, models.Locations,
                models.HistoricalLocations]:
            self.assertEqual(_m.objects.count(), 0)

        # fill database with test data necessary
        models.LocTypes(code="NXS", name="Maven compatible").save()
        models.LocTypes(code="SMB", name="SAMBA").save()
        models.LocTypes(code="SVN", name="SubVersion").save()
        models.CiTypes(code="OTHER", name="Other archive").save()
        models.CiTypes(code="FILE", name="Other File").save()
        models.CsTypes(code="MD5", name="MD5").save()

        self.ck_controller = controllers.CheckSumsController()
        self.app = QueueWorkerApplicationMock(setup_orm=False, controller=self.ck_controller)
        self.rpc = ChecksumsQueueClient()

        self.rpc._Connection = LoopbackConnection
        self.app._Connection = LoopbackConnection

        self.rpc.setup('amqp://127.0.0.1/', queue='my')
        self.rpc.connect()

        self.app.max_sleep = 0
        self.app.main(['--amqp-url', 'amqp://127.0.0.1/', '--declare', 'only', '--queue', 'my'])

        global_message_queue['my'].clear()
        global_message_id['my']=0

    def tearDown(self):
        django.core.management.call_command('flush', verbosity=0, interactive=False)

    def run_main(self, add_args=None):
        _args = [
                '--amqp-url', 'amqp://127.0.0.1/',
                '--reconnect-tries', '0',
                '--reconnect-delay', '0',
                '--remove', 'always',
                '--max-depth', '1',
                '--queue', 'my']

        if add_args:
            _args += add_args

        self.app.main(_args)

    def check_counters(self, **kvargs):
        for (_k, _v) in kvargs.items():
            if not hasattr(models, _k):
                raise TypeError("No such model: '%s'" % _k)

            self.assertEqual(getattr(models, _k).objects.count(), _v)
