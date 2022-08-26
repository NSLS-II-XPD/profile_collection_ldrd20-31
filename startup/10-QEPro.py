from gc import collect
import logging
import time

from ophyd import (Device, Component as Cpt, FormattedComponent as FC,
                   Signal)
from ophyd import (EpicsSignal, EpicsSignalRO, DeviceStatus, DerivedSignal)
from ophyd.areadetector import EpicsSignalWithRBV as SignalWithRBV
from ophyd.status import SubscriptionStatus

class QEProTEC(Device):

    # Thermal electric cooler settings
    tec = Cpt(SignalWithRBV, 'TEC')
    tec_temp = Cpt(SignalWithRBV, 'TEC_TEMP', kind='config')
    curr_tec_temp = Cpt(EpicsSignalRO, 'CURR_TEC_TEMP_RBV')

    def __init__(self, *args, tolerance=1, **kwargs):
        self.tolerance = tolerance
        super().__init__(*args, **kwargs)

    def set(self, value):

        def check_setpoint(value, old_value, **kwargs):
            if abs(value - self.tec_temp.get()) < self.tolerance:
                print(f'Reached setpoint {self.tec_temp.get()}.')
                return True
            return False

        status = SubscriptionStatus(self.curr_tec_temp, run=False, callback=check_setpoint)
        self.tec_temp.put(value)
        self.tec.put(1)

        return status


class QEPro(Device):

    # Device information
    serial = Cpt(EpicsSignal, 'SERIAL')
    model = Cpt(EpicsSignal, 'MODEL')

    # Device Status
    status = Cpt(EpicsSignal, 'STATUS')
    status_msg = Cpt(EpicsSignal, 'STATUS_MSG')
    device_connected = Cpt(EpicsSignalRO, 'CONNECTED_RBV')

    # Utility signal that periodically checks device temps.
    __check_status = Cpt(EpicsSignal, 'CHECK_STATUS')

    # Bit array outlining which features are supported by the device
    features = Cpt(EpicsSignalRO, 'FEATURES_RBV')
    
    # Togglable features (if supported)
    strobe = Cpt(SignalWithRBV, 'STROBE')
    electric_dark_correction = Cpt(SignalWithRBV, 'EDC', kind='config')
    non_linearity_correction = Cpt(SignalWithRBV, 'NLC', kind='config')
    shutter = Cpt(SignalWithRBV, 'SHUTTER')
    
    # Thermal electric cooler
    tec_device = Cpt(QEProTEC, '')
    
    # Light source feature signals
    light_source = Cpt(SignalWithRBV, 'LIGHT_SOURCE')
    light_source_intensity = Cpt(SignalWithRBV, 'LIGHT_SOURCE_INTENSITY')
    light_source_count = Cpt(EpicsSignalRO, 'LIGHT_SOURCE_COUNT_RBV')

    # Signals for specifying the number of spectra to average and counter for spectra
    # collected in current scan
    num_spectra = Cpt(SignalWithRBV, 'NUM_SPECTRA', kind='hinted')
    spectra_collected = Cpt(EpicsSignalRO, 'SPECTRA_COLLECTED_RBV')

    # Integration time settings (in ms)
    int_min_time = Cpt(EpicsSignalRO, 'INT_MIN_TIME_RBV')
    int_max_time = Cpt(EpicsSignalRO, 'INT_MAX_TIME_RBV')
    integration_time = Cpt(SignalWithRBV, 'INTEGRATION_TIME', kind='hinted')
    
    # Internal buffer feature settings
    buff_min_capacity = Cpt(EpicsSignalRO, 'BUFF_MIN_CAPACITY_RBV')
    buff_max_capacity = Cpt(EpicsSignalRO, 'BUFF_MAX_CAPACITY_RBV')
    buff_capacity = Cpt(SignalWithRBV, 'BUFF_CAPACITY')
    buff_element_count = Cpt(EpicsSignalRO, 'BUFF_ELEMENT_COUNT_RBV')

    # Formatted Spectra
    output = Cpt(EpicsSignal, 'OUTPUT', kind='hinted')
    sample = Cpt(EpicsSignal, 'SAMPLE', kind='hinted')
    dark = Cpt(EpicsSignal, 'DARK', kind='hinted')
    reference = Cpt(EpicsSignal, 'REFERENCE', kind='hinted')
    
    # Length of spectrum (in pixels)
    formatted_spectrum_len = Cpt(EpicsSignalRO, 'FORMATTED_SPECTRUM_LEN_RBV')

    # X-axis format and array
    x_axis = Cpt(EpicsSignal, 'X_AXIS', kind='hinted')
    x_axis_format = Cpt(SignalWithRBV, 'X_AXIS_FORMAT')

    # Dark/Ref available signals
    dark_available = Cpt(EpicsSignalRO, 'DARK_AVAILABLE_RBV')
    ref_available = Cpt(EpicsSignalRO, 'REF_AVAILABLE_RBV')

    # Collection settings and start signals.
    acquire = Cpt(SignalWithRBV, 'COLLECT')
    collect_mode = Cpt(SignalWithRBV, 'COLLECT_MODE', kind='hinted')
    spectrum_type = Cpt(SignalWithRBV, 'SPECTRUM_TYPE', kind='hinted')
    correction = Cpt(SignalWithRBV, 'CORRECTION', kind='hinted')
    trigger_mode = Cpt(SignalWithRBV, 'TRIGGER_MODE')


    @property
    def has_nlc_feature(self):
        return self.features.get() & 32

    @property
    def has_lightsource_feature(self):
        return self.features.get() & 16

    @property
    def has_edc_feature(self):
        return self.features.get() & 8

    @property
    def has_buffer_feature(self):
        return self.features.get() & 4

    @property
    def has_tec_feature(self):
        return self.features.get() & 2

    @property
    def has_irrad_feature(self):
        return self.features.get() & 1


    def set_temp(self, temperature):
        self.tec_device.set(temperature).wait()


    def get_dark_frame(self):

        current_spectrum = self.spectrum_type.get()
        self.spectrum_type.put('Dark')
        self.acquire.put(1, wait=True)
        time.sleep(1)
        self.spectrum_type.put(current_spectrum)
    
    def get_reference_frame(self):

        current_spectrum = self.spectrum_type.get()
        self.spectrum_type.put('Reference')
        self.acquire.put(1, wait=True)
        time.sleep(1)
        self.spectrum_type.put(current_spectrum)


    def setup_collection(self, integration_time=100, num_spectra_to_average=10, spectrum_type='Absorbtion', correction_type='Reference', electric_dark_correction=True):
        self.integration_time.put(integration_time)
        self.num_spectra.put(num_spectra_to_average)
        if num_spectra_to_average > 1:
            self.collect_mode.put('Average')
        else:
            self.collect_mode.put('Single')

        if electric_dark_correction:
            self.electric_dark_correction.put(1)

        self.correction.put(correction_type)

        self.spectrum_type.put(spectrum_type)


    def grab_frame(self):

        def is_done(value, old_value, **kwargs):
            if old_value == 1 and value ==0:
                return True
            return False

        status = SubscriptionStatus(self.acquire, run=False, callback=is_done)

        self.acquire.put(1)
        return status

    def trigger(self):
        self.grab_frame().wait()


qepro = QEPro('XF:28ID2-ES{QEPro:Spec-1}:', name='QEPro')
