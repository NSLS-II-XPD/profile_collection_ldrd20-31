from ophyd.sim import det, noisy_det
from bluesky.utils import ts_msg_hook
RE.msg_hook = ts_msg_hook


def outer_plan(det1, det2, *args, md={}, num_fast=10, **kwargs):
    @bpp.stage_decorator([det1, det2])
    @bpp.run_decorator(md=md)
    def trigger_two_detectors():  # TODO: rename appropriately
        yield from bps.trigger(det1)

        ret = {}

        # TODO: write your fast procedure here, don't use bp.count/bp.scan here as they open separate runs.
        #   Use `trigger_and_read` instead.
        for i in range(num_fast):
            yield from bps.trigger(det2)

            yield from bps.create(name="absorbance")
            reading = (yield from bps.read(det2))
            print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.

        for i in range(num_fast * 2):  # TODO: fix the number of triggers
            yield from bps.trigger(det2)

            yield from bps.create(name="fluorescence")
            reading = (yield from bps.read(det2))
            print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.


        # yield from bps.sleep(3)
        ...
        ###

        yield from bps.create(name="slow")
        reading = (yield from bps.read(det1))
        print(f"reading = {reading}")
        ret.update(reading)
        yield from bps.save()
    
    yield from trigger_two_detectors()
