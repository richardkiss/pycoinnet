import asyncio


def pipeline(transformation_f, q=None, stop_on_None=True, loop=None):
    # fetch things from in_q
    # invoke transformation_f in a task when they arrive
    # when we get a None from in_q, it's time to stop

    q = q or asyncio.Queue()
    loop = loop or asyncio.get_event_loop()

    pending = set()

    process_task = None

    def close(cancel_pending=False):
        nonlocal pending
        if cancel_pending:
            for p in pending:
                if not p.done():
                    p.cancel()
        if not process_task.done():
            process_task.cancel()
        gather_list = [process_task] + list(pending)
        pending = set()
        return asyncio.gather(*gather_list)

    async def _process_task(close):
        try:
            while True:
                item = await q.get()
                if item is None and stop_on_None:
                    break
                task = loop.create_task(transformation_f(item))
                pending.add(task)
                done = set(p for p in pending if p.done())
                pending.difference(done)
            loop.call_soon(close)
        except asyncio.CancelledError:
            pass

    process_task = loop.create_task(_process_task(close))

    q.close = close
    return q
