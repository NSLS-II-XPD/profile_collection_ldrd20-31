from gc import collect
import logging
import time

import epics

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


    def setup_collection(self, integration_time=100, num_spectra_to_average=10, 
                         spectrum_type='Absorbtion', correction_type='Reference', 
                         electric_dark_correction=True):
        
        # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
        # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'
        
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
        #self.grab_frame().wait()
        return self.grab_frame()

    def write_as_csv(self, write_path):
    
        print(f'Writing out CSV file to {write_path}...')

        with open(write_path, 'w') as fp:
            x_axis_data = self.x_axis.get()
            output_data = self.output.get()
            sample_data = self.sample.get()
            dark_data = self.dark.get()
            reference_data = self.reference.get()
            if self.spectrum_type.get(as_string=True) == 'Absorbtion':
                fp.write('Energy,Dark,Reference,Sample,Absorbtion\n')
            else:
                fp.write('Energy,Dark,Raw Sample,Corrected Sample\n')

            for i in range(len(output_data)):
                if self.spectrum_type.get(as_string=True) == 'Absorbtion':
                    fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
                else:
                    fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')

            print('Done.')

    def plot_spectra(self):
        x_axis_data = self.x_axis.get()
        output_data = self.output.get()

        x_axis_label = self.x_axis_format.get(as_string=True)
        y_axis_label = self.spectrum_type.get(as_string=True)


        plt.plot(x_axis_data, output_data)
        plt.xlabel(x_axis_label)
        plt.ylabel(y_axis_label)
        plt.show()
        


    def take_uvvis_save_csv(self, sample_type='test', plot=False, csv_path=None, 
                            spectrum_type='Absorbtion', correction_type='Reference', 
                            pump_list=None, precursor_list=None):
        
        # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
        # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'
        
        # self.correction.put(correction_type)
        # self.spectrum_type.put(spectrum_type)
        
        if spectrum_type == 'Absorbtion':
            if LED.get()=='Low' and UV_shutter.get()=='High' and self.correction.get()==correction_type and self.spectrum_type.get()==spectrum_type:
                uid = (yield from count([self]))
            else:
                self.correction.put(correction_type)
                self.spectrum_type.put(spectrum_type)
                yield from LED_off()
                yield from shutter_open()
                uid = (yield from count([self]))
            
            
        else:
            if LED.get()=='High' and UV_shutter.get()=='Low' and self.correction.get()==correction_type and self.spectrum_type.get()==spectrum_type:
                uid = (yield from count([self]))
            else:
                self.correction.put(correction_type)
                self.spectrum_type.put(spectrum_type)
                yield from shutter_close()
                yield from LED_on()       
                uid = (yield from count([self]))
            
        unix_time = db[uid].start['time']     
        date, time = _readable_time(unix_time)
        x_axis_data = db[uid].table().QEPro_x_axis[1]
        output_data = db[uid].table().QEPro_output[1]
        
        if plot == True:
            x_axis_label = self.x_axis_format.get(as_string=True)
            y_axis_label = spectrum_type

            plt.plot(x_axis_data, output_data)
            plt.xlabel(x_axis_label)
            plt.ylabel(y_axis_label)
            plt.show()
          
        if csv_path != None:

            fout = f'{csv_path}/{sample_type}_{spectrum_type[0:3]}_{date}-{time}_{uid[0:8]}.csv'
            
            with open(fout, 'w') as fp:
                if pump_list != None:
                    for pump, precursor in zip(pump_list, precursor_list):
                        fp.write(f'{pump.name},{precursor},{pump.read_infuse_rate.get()},{pump.read_infuse_rate_unit.get()},{pump.status.get()}\n')
                
                fp.write(f'Time_QEPro,{date},{time}\n')

                sample_data = db[uid].table().QEPro_sample[1]
                dark_data = db[uid].table().QEPro_dark[1]
                reference_data = db[uid].table().QEPro_reference[1]

                if spectrum_type == 'Absorbtion':
                    fp.write('Energy,Dark,Reference,Sample,Absorbance\n')
                else:
                    fp.write('Energy,Dark,Raw Sample,Fluorescence\n')

                for i in range(len(output_data)):
                    if spectrum_type == 'Absorbtion':
                        fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
                    else:
                        fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')
        
        

    def take_uvvis_save_csv2(self, sample_type='test', plot=False, csv_path=None, 
                            spectrum_type='Absorbtion', correction_type='Reference', 
                            pump_list=None, precursor_list=None):
        
        # For absorbance: spectrum_type='Absorbtion', correction_type='Reference'
        # For fluorescence: spectrum_type='Corrected Sample', correction_type='Dark'
        
        # self.correction.put(correction_type)
        # self.spectrum_type.put(spectrum_type)
        
        if spectrum_type == 'Absorbtion':
            if LED.get()=='Low' and UV_shutter.get()=='High' and self.correction.get()==correction_type and self.spectrum_type.get()==spectrum_type:
                uid = (yield from count([self]))
            else:
                self.correction.put(correction_type)
                self.spectrum_type.put(spectrum_type)
                yield from LED_off()
                yield from shutter_open()
                uid = (yield from count([self]))
            
            
        else:
            if LED.get()=='High' and UV_shutter.get()=='Low' and self.correction.get()==correction_type and self.spectrum_type.get()==spectrum_type:
                uid = (yield from count([self]))
            else:
                self.correction.put(correction_type)
                self.spectrum_type.put(spectrum_type)
                yield from shutter_close()
                yield from LED_on()       
                uid = (yield from count([self]))
            
        
        run = tiled_client[uid]
        ds = run.primary.read()          
        date, time = _readable_time(ds['time'][0])
        x_axis_data = ds['QEPro_x_axis'].values[0]
        output_data = ds['QEPro_output'].values[0]
        
        if plot == True:
            x_axis_label = self.x_axis_format.get(as_string=True)
            y_axis_label = spectrum_type

            plt.plot(x_axis_data, output_data)
            plt.xlabel(x_axis_label)
            plt.ylabel(y_axis_label)
            plt.show()
          
        if csv_path != None:

            fout = f'{csv_path}/{sample_type}_{spectrum_type[0:3]}_{date}-{time}_{uid[0:8]}.csv'
            
            with open(fout, 'w') as fp:
                if pump_list != None:
                    for pump, precursor in zip(pump_list, precursor_list):
                        fp.write(f'{pump.name},{precursor},{pump.read_infuse_rate.get()},{pump.read_infuse_rate_unit.get()},{pump.status.get()}\n')
                
                fp.write(f'Time_QEPro,{date},{time}\n')

                sample_data = ds['QEPro_sample'].values[0]
                dark_data = ds['QEPro_dark'].values[0]
                reference_data = ds['QEPro_reference'].values[0]

                if spectrum_type == 'Absorbtion':
                    fp.write('Energy,Dark,Reference,Sample,Absorbance\n')
                else:
                    fp.write('Energy,Dark,Raw Sample,Fluorescence\n')

                for i in range(len(output_data)):
                    if spectrum_type == 'Absorbtion':
                        fp.write(f'{x_axis_data[i]},{dark_data[i]},{reference_data[i]},{sample_data[i]},{output_data[i]}\n')
                    else:
                        fp.write(f'{x_axis_data[i]},{dark_data[i]},{sample_data[i]},{output_data[i]}\n')
            

# from tiled.client import from_profile
# tiled_client = from_profile("xpd")
qepro = QEPro('XF:28ID2-ES{QEPro:Spec-1}:', name='QEPro', )

