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



def _qserver_inputs():
    qserver_list=[
            'dummy_qserver', 'name_by_prefix', 'prefix', 
            'pump_list', 'precursor_list', 'syringe_mater_list', 'syringe_list', 
            'target_vol_list', 'sample', 'mixer', 'wash_tube', 'resident_t_ratio', 
            'rate_unit', 'uvvis_config', 'perkin_config', 
            'set_target_list', 'infuse_rates', 
            ]

    return qserver_list


def _kafka_process():
    kafka_list=[
            'dummy_kafka', 'csv_path', 'key_height', 'height', 'distance', 'PLQY', 
            'rate_label_dic_key', 'rate_label_dic_value', 'new_points_label', 
            'use_good_bad', 'post_dilute', 'write_agent_data', 'agent_data_path', 
            'USE_AGENT_iterate', 'peak_target',  
            'iq_to_gr', 'iq_to_gr_path', 'cfg_fn', 'bkg_fn', 'iq_fn',  
            'search_and_match', 'mystery_path', 'results_path', 
            'fitting_pdf', 'fitting_pdf_path', 'cif_fn', 'gr_fn', 
            'use_sandbox', 'write_to_sandbox', 'sandbox_tiled_client', 
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

        ## Append agent in the list of self.inputs.agent
        if self.inputs.agent==[]:
            self.inputs.agent.append(
                build_agen(
                    peak_target=self.inputs.peak_target, 
                    agent_data_path=self.inputs.agent_data_path)
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

                




    # def add_to_queue(self):
    #     sq.synthesis_queue(
    #                 syringe_list=self.input_dic.syringe_list, 
    #                 pump_list=self.input_dic.pump_list, 
    #                 set_target_list=self.input_dic.set_target_list, 
    #                 target_vol_list=self.input_dic.target_vol_list, 
    #                 rate_list = self.input_dic.infuse_rates, 
    #                 syringe_mater_list=self.input_dic.syringe_mater_list, 
    #                 precursor_list=self.input_dic.precursor_list,
    #                 mixer=self.input_dic.mixer, 
    #                 resident_t_ratio=self.input_dic.resident_t_ratio, 
    #                 prefix=self.input_dic.prefix[1:], 
    #                 sample=self.input_dic.sample, 
    #                 wash_tube=self.input_dic.wash_tube, 
    #                 name_by_prefix=bool(self.input_dic.prefix[0]),  
	# 				num_abs=self.input_dic.num_uvvis[0], 
	# 				num_flu=self.input_dic.num_uvvis[1],
    #                 det1_time=self.input_dic.num_uvvis[2], 
    #                 zmq_control_addr=self.input_dic.zmq_control_addr, 
	# 				zmq_info_addr=self.input_dic.zmq_info_addr, 
    #                 )

'''
class kafka():
    def __init__(self):
        self.rate_label_dic = {'CsPb':'infusion_rate_CsPb', 
                                'Br':'infusion_rate_Br', 
                                'ZnI':'infusion_rate_I2', 
                                'ZnCl':'infusion_rate_Cl'}
        self.new_points_label = ['infusion_rate_CsPb', 'infusion_rate_Br', 'infusion_rate_I2', 'infusion_rate_Cl']
        
        self.use_good_bad = True
        self.post_dilute = True
        
        self.write_agent_data = True
        self.agent_data_path = '/home/xf28id2/Documents/ChengHung/202405_halide_data/with_xray'

        self.USE_AGENT_iterate = False
        self.peak_target = 515
        self.agent = build_agen(peak_target=self.peak_target, agent_data_path=self.agent_data_path)

        self.iq_to_gr = False
        self.gr_path = '/home/xf28id2/Documents/ChengHung/pdfstream_test/'
        self.cfg_fn = '/home/xf28id2/Documents/ChengHung/pdfstream_test/pdfgetx3.cfg'
        self.iq_fn = glob.glob(os.path.join(gr_path, '**CsPb**.chi'))
        # self.bkg_fn = glob.glob(os.path.join(gr_path, '**bkg**.chi'))
        self.bkg_fn = ['/nsls2/data/xpd-new/legacy/processed/xpdUser/tiff_base/Toluene_OleAcid_mask/integration/Toluene_OleAcid_mask_20240602-122852_c49480_primary-1_mean_q.chi']




USE_AGENT_iterate = False
peak_target = 515
if USE_AGENT_iterate:
    import torch
    from prepare_agent_pdf import build_agen
    agent = build_agen(peak_target=peak_target, agent_data_path=agent_data_path)

iq_to_gr = True
if iq_to_gr:
    from diffpy.pdfgetx import PDFConfig
    global gr_path, cfg_fn, iq_fn, bkg_fn
    gr_path = '/home/xf28id2/Documents/ChengHung/pdfstream_test/'
    cfg_fn = '/home/xf28id2/Documents/ChengHung/pdfstream_test/pdfgetx3.cfg'
    iq_fn = glob.glob(os.path.join(gr_path, '**CsPb**.chi'))
    # bkg_fn = glob.glob(os.path.join(gr_path, '**bkg**.chi'))
    bkg_fn = ['/nsls2/data/xpd-new/legacy/processed/xpdUser/tiff_base/Toluene_OleAcid_mask/integration/Toluene_OleAcid_mask_20240602-122852_c49480_primary-1_mean_q.chi']
    
search_and_match = True
if search_and_match:
    from updated_pipeline_pdffit2 import Refinery
    mystery_path = "/home/xf28id2/Documents/ChengHung/pdffit2_example/CsPbBr3"
    # mystery_path = "'/home/xf28id2/Documents/ChengHung/pdfstream_test/gr"
    results_path = "/home/xf28id2/Documents/ChengHung/pdffit2_example/results_CsPbBr_chemsys_search"

fitting_pdf = False
if fitting_pdf:
    global pdf_cif_dir, cif_list, gr_data
    pdf_cif_dir = '/home/xf28id2/Documents/ChengHung/pdffit2_example/CsPbBr3/'
    cif_list = [os.path.join(pdf_cif_dir, 'CsPbBr3_Orthorhombic.cif')]
    gr_data = os.path.join(pdf_cif_dir, 'CsPbBr3.gr')

global sandbox_tiled_client
use_sandbox = True
if use_sandbox:
    sandbox_tiled_client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/xpd/sandbox")

write_to_sandbox = False
if write_to_sandbox:
    sandbox_tiled_client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/xpd/sandbox")
'''




