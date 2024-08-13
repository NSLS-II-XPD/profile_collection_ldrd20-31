from ophyd.sim import det, noisy_det
from bluesky.utils import ts_msg_hook
import bluesky.preprocessors as bpp
# import time
# RE.msg_hook = ts_msg_hook




def xray_uvvis_plan2(det1, det2, *args, md=None, num_abs=10, num_flu=10, sample_type = 'test',
                    pump_list=None, precursor_list=None, mixer=None, note=None, **kwargs):
    """Trigger the two detctors (det1: pe1c, det2: qepro): det2 first and then det1.
        
    Generate a scan containing three stream names: ['scattering', 'absorbance', 'fluorescence']

    Args:
        det1 (ophyd.Device): xray detector (example: pe1c)
        det2 (ophyd.Device): Uv-Vis detector (example: qepro)
        md (dict, optional): metadata.
        num_abs (int, optional): numbers of absorption spectra
        num_flu (int, optional): numbers of fluorescence spectra
        sample_type (str, optional): sample name
        pump_list (list, optional): list of pumps as ophyd.Device (example: [dds1_p1, dds1_p2, dds2_p1, dds2_p2])
        precursor_list (list, optional): list of precursors name (example: ['CsPbOA', 'TOABr', 'ZnI2', 'Toluene'])
        mixer (list, optional): list of mixers (example: ['30 cm', '60 cm'])
        note (str, optional): addtional info. Defaults to None.
    """
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list],
               "precursors" : precursor_list,
               "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list],
               "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
               "pump_status" : [pump.status.get() for pump in pump_list],
               "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()],
               "mixer": mixer,
               "sample_type": sample_type,
               "sample_name": sample_type,
               "detectors": [det1.name, det2.name], 
               "note" : note if note else "None"}
        _md.update(md or {})
        dilute_pump = pump_list[-1]

    if (pump_list == None and precursor_list == None):
        _md = { "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()],
                "mixer": ['exsitu measurement'],
                "sample_type": sample_type, 
                "sample_name": sample_type,
                "detectors": [det1.name, det2.name], 
                "note" : note if note else "None"}
        _md.update(md or {})

    
    @bpp.stage_decorator([det1, det2])
    @bpp.run_decorator(md=_md)
    def trigger_two_detectors():  # TODO: rename appropriately
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
        if LED.get()=='Low' and UV_shutter.get()=='High' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.mv(LED, 'Low', UV_shutter, 'High')
            yield from bps.sleep(2)

        for i in range(num_abs):
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="absorbance")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)


        ## Start to collecting fluorescence
        spectrum_type='Corrected Sample'
        correction_type='Dark'
        if LED.get()=='High' and UV_shutter.get()=='Low' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.syield from bps.sleep(xray_time)pectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.mv(LED, 'High', UV_shutter, 'Low')
            yield from bps.sleep(2)

        for i in range(num_flu):  # TODO: fix the number of triggers
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="fluorescence")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)

        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')

        if (pump_list != None) and (precursor_list != None):
            yield from stop_group([dilute_pump])
        
        print(f'\nUV-Vis acquisition finished and stop infusing of {dilute_pump.name} for toluene dilution\n')
        
       
        ## Start to collecting scattering
        yield from bps.trigger(det1, wait=True)
        # yield from bps.sleep(det1_time)
        yield from bps.create(name="scattering")
        reading = (yield from bps.read(det1))
        print(f"reading = {reading}")
        ret.update(reading)
        yield from bps.save()


    yield from trigger_two_detectors()






def xray_uvvis_plan(det1, det2, *args, md=None, num_abs=10, num_flu=10, sample_type = 'test', 
                    pump_list=None, precursor_list=None, mixer=None, note=None, **kwargs):
    
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list], 
               "precursors" : precursor_list, 
               "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list], 
               "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
               "pump_status" : [pump.status.get() for pump in pump_list], 
               "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()], 
               "mixer": mixer,
               "sample_type": sample_type, 
               "sample_name": sample_type, 
               "detectors": [det1.name, det2.name],
               "note" : note if note else "None"}
        _md.update(md or {})    
        
    if (pump_list == None and precursor_list == None):
        _md = { "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()], 
                "mixer": ['exsitu measurement'],
                "sample_type": sample_type, 
                "sample_name": sample_type, 
                "detectors": [det1.name, det2.name],
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
        if LED.get()=='Low' and UV_shutter.get()=='High' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.sleep(1)
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
        
        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
        yield from bps.sleep(1)

        ## Start to collecting fluorescence
        spectrum_type='Corrected Sample'
        correction_type='Dark'
        if LED.get()=='High' and UV_shutter.get()=='Low' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.sleep(1)
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

        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
        yield from bps.sleep(1)
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




def xray_uvvis_plan3(det1, det2, *args, md=None, num_abs=10, num_flu=10, sample_type = 'test', 
                    pump_list=None, precursor_list=None, mixer=None, note=None, **kwargs):
    
    if (pump_list != None and precursor_list != None):
        _md = {"pumps" : [pump.name for pump in pump_list], 
               "precursors" : precursor_list, 
               "infuse_rate" : [pump.read_infuse_rate.get() for pump in pump_list], 
               "infuse_rate_unit" : [pump.read_infuse_rate_unit.get() for pump in pump_list],
               "pump_status" : [pump.status.get() for pump in pump_list], 
               "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()], 
               "mixer": mixer,
               "sample_type": sample_type,
               "detectors": [det1.name, det2.name],
               "note" : note if note else "None"}
        _md.update(md or {})    
        
    if (pump_list == None and precursor_list == None):
        _md = { "uvvis" :[det2.integration_time.get(), det2.num_spectra.get(), det2.buff_capacity.get()], 
                "mixer": ['exsitu measurement'],
                "sample_type": sample_type,
                "detectors": [det1.name, det2.name],
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
        if LED.get()=='Low' and UV_shutter.get()=='High' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.sleep(1)
            yield from bps.mv(LED, 'Low', UV_shutter, 'High')
            yield from bps.sleep(1)

        for i in range(num_abs):
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="absorbance3")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)
        
        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
        yield from bps.sleep(1)

        ## Start to collecting fluorescence
        spectrum_type='Corrected Sample'
        correction_type='Dark'
        if LED.get()=='High' and UV_shutter.get()=='Low' and det2.correction.get()==correction_type and det2.spectrum_type.get()==spectrum_type:
            pass
        else:
            # yield from bps.abs_set(qepro.correction, correction_type, wait=True)
            # yield from bps.abs_set(qepro.spectrum_type, spectrum_type, wait=True)
            yield from bps.mv(det2.correction, correction_type, det2.spectrum_type, spectrum_type)
            yield from bps.sleep(1)
            yield from bps.mv(LED, 'High', UV_shutter, 'Low')
            yield from bps.sleep(1)

        for i in range(num_flu):  # TODO: fix the number of triggers
            yield from bps.trigger(det2, wait=True)

            yield from bps.create(name="fluorescence3")
            reading = (yield from bps.read(det2))
            # print(f"reading = {reading}")
            ret.update(reading)
            yield from bps.save()  # TODO: check if it's needed, most likely yes.
            # yield from bps.sleep(2)

        yield from bps.mv(LED, 'Low', UV_shutter, 'Low')
        yield from bps.sleep(1)
        # t1 = time.time()
        # yield from bps.sleep(3)
        ...
        ###

        yield from bps.create(name="scattering3")
        reading = (yield from bps.read(det1))
        print(f"reading = {reading}")
        ret.update(reading)
        yield from bps.save()

    yield from trigger_two_detectors()
