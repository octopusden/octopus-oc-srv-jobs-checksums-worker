from oc_cdt_queue2.test.synchron.mocks.queue_application import QueueApplication
from ...checksums_worker import QueueWorkerApplication


class QueueWorkerApplicationMock(QueueWorkerApplication, QueueApplication):
    connect = QueueApplication.connect
    run = QueueApplication.run
    main = QueueApplication.main
    _connect_and_run = QueueApplication._connect_and_run
    _connection_prcs = None
