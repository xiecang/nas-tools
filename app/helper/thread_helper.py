import log
import traceback
from concurrent.futures import ThreadPoolExecutor

from app.utils.commons import singleton


@singleton
class ThreadHelper:
    _thread_num = 50
    executor = None

    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=self._thread_num)

    def init_config(self):
        pass

    def thread_pool_callback(self, worker):
        e = worker.exception()
        if e is None:
            return
        tb = e.__traceback__
        log.debug("【Exception】线程执行异常，错误: " + "".join(traceback.format_tb(tb)))

    def start_thread(self, func, kwargs):
        thread_pool_exc = self.executor.submit(func, *kwargs)
        thread_pool_exc.add_done_callback(self.thread_pool_callback)
