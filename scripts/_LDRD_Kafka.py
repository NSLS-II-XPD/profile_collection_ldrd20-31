import os
import glob
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da
# import _synthesis_queue_RM as sq
# import _pdf_calculator as pc
# import _get_pdf as gp

# import torch
# from prepare_agent_pdf import build_agen
# from diffpy.pdfgetx import PDFConfig
# from tiled.client import from_uri



def _qserver_inputs():
    qserver_list=[
            'zmq_control_addr', 'zmq_info_addr', 
            'dummy_qserver', 'is_iteration', 'pos', 
            'name_by_prefix', 'prefix', 'pump_list', 'precursor_list', 
            'syringe_mater_list', 'syringe_list', 'target_vol_list', 
            'sample', 
            'wait_dilute', 'mixer', 'wash_tube', 'resident_t_ratio', 
            'rate_unit', 'uvvis_config', 'perkin_config', 
            'set_target_list', 'infuse_rates', 
            ]

    return qserver_list


def _kafka_process():
    kafka_list=[
            'dummy_kafka', 'csv_path', 'key_height', 'height', 'distance', 'PLQY', 
            'rate_label_dic_key', 'rate_label_dic_value', 'new_points_label', 
            'use_good_bad', 'post_dilute', 'write_agent_data', 'agent_data_path', 
            'USE_AGENT_iterate', 'peak_target', 'agent', 
            'iq_to_gr', 'iq_to_gr_path', 'cfg_fn', 'bkg_fn', 'iq_fn',  
            'search_and_match', 'mystery_path', 'results_path', 
            'fitting_pdf', 'fitting_pdf_path', 'cif_fn', 'gr_fn', 
            'use_sandbox', 'write_to_sandbox', 'sandbox_tiled_client', 'tiled_client', 
            'fn_TBD', 
            ]

    return kafka_list



class dic_to_inputs():
    def __init__(self, parameters_dict, parameters_list):
        self.parameters_dict = parameters_dict
        self.parameters_list = parameters_list

        for key in self.parameters_list:
            # print(f'{key = }')
            try:
                setattr(self, key, self.parameters_dict[key])
            except KeyError:
                setattr(self, key, [])
                print(f'{key = } not in parameters_dict so set to empty list.')


class xlsx_to_inputs():
    def __init__(self, parameters_list, xlsx_fn, sheet_name='inputs'):
        self.parameters_list = parameters_list
        self.from_xlsx = xlsx_fn
        self.sheet_name = sheet_name
        self.print_dic = de._read_input_xlsx(self.from_xlsx, sheet_name=self.sheet_name)
        
        ## Every attribute in self.inputs is a list!!!
        self.inputs = dic_to_inputs(self.print_dic, self.parameters_list)

        try:
            ## Append agent in the list of self.inputs.agent
            if self.inputs.agent==[]:
                self.inputs.agent.append(
                    build_agen(
                        peak_target=self.inputs.peak_target[0], 
                        agent_data_path=self.inputs.agent_data_path[0])
                        )

            ## self.inputs.sandbox_tiled_client[0] is just the uri of sandbox
            ## so, turn uri into client and append it in self.inputs.sandbox_tiled_client
            if type(self.inputs.sandbox_tiled_client[0]) is str:
                self.inputs.sandbox_tiled_client.append(from_uri(self.inputs.sandbox_tiled_client[0]))


            ## Use glob.glob to find the complete path of cfg_fn, bkg_fn, iq_fn, cif_fn, gr_fn
            # fn_TBD = ['cfg_fn', 'bkg_fn', 'iq_fn', 'cif_fn', 'gr_fn']
            for fn in self.inputs.fn_TBD:
                
                path = getattr(self.inputs, fn)[0]
                if path in self.parameters_list:
                    path = getattr(self.inputs, path)[0]
                
                ff = getattr(self.inputs, fn)[1]

                fn_glob = glob.glob(os.path.join(path, ff))

                for i in fn_glob:
                    getattr(self.inputs, fn).append(i)
        
        except AttributeError:
            pass

                
