from ophyd.sim import det, noisy_det
from bluesky.utils import ts_msg_hook
import bluesky.preprocessors as bpp
# import time
# RE.msg_hook = ts_msg_hook


def xray_uvvis_plan(det1, det2, *args, md=None, num_abs=10, num_flu=10, sample_type = 'test', 
                    pump_list=None, precursor_list=None, mixer=None, note=None, **kwargs):
    
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list], 
               "precursors" : precursor_list, 
               "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list], 
               "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
               "pump_status" : [pump.status.get() for pump in pump_list], 
               "uvvis" :[qepro.integration_time.get(), qepro.num_spectra.get(), qepro.buff_capacity.get()], 
               "mixer": mixer,
               "sample_type": sample_type,
               "note" : note if note else "None"}
        _md.update(md or {})    
        
    if (pump_list == None and precursor_list == None):
        _md = { "uvvis" :[qepro.integration_time.get(), qepro.num_spectra.get(), qepro.buff_capacity.get()], 
                "mixer": ['exsitu measurement'],
                "sample_type": sample_type,
                "note" : note if note else "None"}
        _md.update(md or {})


    @bpp.stage_decorator([det1, det2])
    @bpp.run_decorator(md=_md)
    def trigger_two_detectors():  # TODO: rename appropriately        
        yield from bps.trigger(det1)

        ret = {}

        # TODO: write your fast procedure here, don't use bp.count/bp.scan here as they open separate runs.
        # Use `trigger_and_read` instead.
        # Tested on 2023/02/16: bps.trigger works for qepro
        
        
        # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
        # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'
        
        ## Start to collecting absrobtion
        # t0 = time.time()
        spectrum_type='Absorbtion'
        correction_type='Reference'
        if LED.get()=='Low' and UV_shutter.get()=='High' and qepro.correction.get()==correction_type and qepro.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(qepro.correction, correction_type, qepro.spectrum_type, spectrum_type)
            yield from bps.mv(LED, 'Low', UV_shutter, 'High')
            yield from bps.sleep(1)

        for i in range(num_abs):
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="absorbance")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)
        
        yield from bps.sleep(1)

        ## Start to collecting fluorescence
        spectrum_type='Corrected Sample'
        correction_type='Dark'
        if LED.get()=='High' and UV_shutter.get()=='Low' and qepro.correction.get()==correction_type and qepro.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(qepro.correction, correction_type, qepro.spectrum_type, spectrum_type)
            yield from bps.mv(LED, 'High', UV_shutter, 'Low')
            yield from bps.sleep(1)

        for i in range(num_flu):  # TODO: fix the number of triggers
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="fluorescence")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)

        yield from bps.sleep(1)
        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
        # t1 = time.time()
        # yield from bps.sleep(3)
        ...
        ###

        yield from bps.create(name="scattering")
        reading = (yield from bps.read(det1))
        print(f"reading = {reading}")
        ret.update(reading)
        yield from bps.save()

    yield from trigger_two_detectors()
