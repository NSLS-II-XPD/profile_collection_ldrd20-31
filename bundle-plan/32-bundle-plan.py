from ophyd.sim import det, noisy_det
from bluesky.utils import ts_msg_hook
RE.msg_hook = ts_msg_hook


def outer_plan(det1, det2, *args, md=None, num_fast=10, sample_type = 'test', 
               spectrum_type='Absorbtion', correction_type='Reference', 
               pump_list=None, precursor_list=None, mixer=None, note=None, **kwargs):
    
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list], 
               "precursors" : precursor_list, 
               "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list], 
               "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
               "pump_status" : [pump.status.get() for pump in pump_list], 
               "uvvis" :[spectrum_type, correction_type, qepro.integration_time.get(), qepro.num_spectra.get()], 
               "mixer": mixer,
               "sample_type": sample_type,
               "note" : note if note else "None"}
        _md.update(md or {})    
    
    @bpp.stage_decorator([det1, det2])
    @bpp.run_decorator(md=_md)
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

        yield from bps.create(name="scattering")
        reading = (yield from bps.read(det1))
        print(f"reading = {reading}")
        ret.update(reading)
        yield from bps.save()

    yield from trigger_two_detectors()
