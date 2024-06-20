import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da
import _synthesis_queue_RM as sq
import _pdf_calculator as pc
import _get_pdf as gp

import torch
from prepare_agent_pdf import build_agen
from diffpy.pdfgetx import PDFConfig



def _Qparameters():
    # qserver_list=['dummy_kafka', 'dummy_qserver', 'csv_path', 
    qserver_list=['key_height', 'height', 'distance', 
                'pump_list', 'precursor_list', 'syringe_mater_list', 'syringe_list', 
                'target_vol_list', 'set_target_list', 'infuse_rates', 'sample', 'mixer', 
                'wash_tube', 'resident_t_ratio', 'PLQY', 'prefix', 'num_uvvis']

    return qserver_list

'''
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
'''

# class input_dic():
#     pass


class xlsx_to_qserver():
    class input_dic():
        pass
    def __init__(self, xlsx_fn, sheet_name='inputs'):
        self.from_xlsx = xlsx_fn
        self.sheet_name = sheet_name
        self.print_dic = de._read_input_xlsx(self.from_xlsx, sheet_name=self.sheet_name)

        self.input_dic = input_dic()
        self.input_dic.dummy_kafka = bool(self.print_dic['dummy_test'][0])
        self.input_dic.dummy_qserver = bool(self.print_dic['dummy_test'][1])
        self.input_dic.csv_path = self.print_dic['csv_path'][0]

        for key in _Qparameters():
            setattr(self.input_dic, key, self.print_dic[key])

        if self.input_dic.prefix:
            self.input_dic.sample = de._auto_name_sample(self.input_dic.infuse_rates, prefix=self.input_dic.prefix[1:])

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



'''
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




