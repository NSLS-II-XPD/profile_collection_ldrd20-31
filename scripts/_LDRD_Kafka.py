import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da
import _pdf_calculator as pc
import _get_pdf as gp



def _auto_keys():
    qepro_list=['QEPro_x_axis', 'QEPro_output', 'QEPro_sample', 'QEPro_dark', 'QEPro_reference', 
                'QEPro_spectrum_type', 'QEPro_integration_time', 'QEPro_num_spectra', 'QEPro_buff_capacity']
    qepro_dic = {}

    metadata_list=['uid', 'time', 'pumps','precursors','infuse_rate','infuse_rate_unit',
                   'pump_status', 'mixer','sample_type', 'note']
    metadata_dic = {}

    return qepro_list, qepro_dic, metadata_list, metadata_dic


dummy_kafka = bool(input_dic['dummy_test'][0])
dummy_qserver = bool(input_dic['dummy_test'][1])
csv_path = input_dic['csv_path'][0]
key_height = input_dic['key_height']
height = input_dic['height']
distance = input_dic['distance']
pump_list = input_dic['pump_list']
precursor_list = input_dic['precursor_list']
syringe_mater_list = input_dic['syringe_mater_list']
syringe_list = input_dic['syringe_list']
target_vol_list = input_dic['target_vol_list']
set_target_list = input_dic['set_target_list']
infuse_rates = input_dic['infuse_rates']
sample = input_dic['sample']
mixer = input_dic['mixer']
wash_tube = input_dic['wash_tube']
resident_t_ratio = input_dic['resident_t_ratio']
PLQY = input_dic['PLQY']
prefix = input_dic['prefix']
num_uvvis = input_dic['num_uvvis']

class make_inputs_as_dict():
    def __init__(self, fn=None):
        self.from_fn = fn


class xlsx_to_qserver(xlsx_fn, )

