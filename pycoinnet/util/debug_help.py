"""
Provide a version of asyncio that monkey-patches Task to
break on exceptions.
"""

import asyncio
import logging
import sys
import traceback

asyncio_Task = asyncio.Task

def _done_callback(f):
    try:
        if f.cancelled():
            return
        ex = f.exception()
        if ex:
            raise ex
        r = f.result()
    except Exception as ex:
        logging.exception("task raised exception")
        print("exception!! =>", ex)
        traceback.print_tb(ex.__traceback__)
        import pdb; pdb.set_trace()
        print(traceback.format_exc())
        #sys.excepthook(ex)

def Task(*args, **kwargs):
    f = asyncio_Task(*args, **kwargs)
    f.add_done_callback(_done_callback)
    return f

asyncio.Task = Task
