import os
import datetime
import pprint
import uuid
# from bluesky_kafka import RemoteDispatcher
from bluesky_kafka.consume import BasicConsumer
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import integrate
import time
import databroker
import json
import glob
from tqdm import tqdm
from diffpy.pdfgetx import PDFConfig
from tiled.client import from_uri, from_profile

import resource
resource.setrlimit(resource.RLIMIT_NOFILE, (65536, 65536))

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da
import _pdf_calculator as pc
import _get_pdf as gp

from bluesky_queueserver_api.zmq import REManagerAPI
from bluesky_queueserver_api import BPlan, BInst

try:
    from nslsii import _read_bluesky_kafka_config_file  # nslsii <0.7.0
except (ImportError, AttributeError):
    from nslsii.kafka_utils import _read_bluesky_kafka_config_file  # nslsii >=0.7.0

# these two lines allow a stale plot to remain interactive and prevent
# the current plot from stealing focus.  thanks to Tom:
# https://nsls2.slack.com/archives/C02D9V72QH1/p1674589090772499
plt.ion()
plt.rcParams["figure.raise_window"] = False

## Input varaibales: read from inputs_qserver_kafka.xlsx
xlsx = '/home/xf28id2/Documents/ChengHung/inputs_qserver_kafka_ML.xlsx'
input_dic = de._read_input_xlsx(xlsx, sheet_name='inputs')

##################################################################
# Define namespace for tasks in Qserver and Kafaka
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
###################################################################
## Add tasks into Qsever
zmq_control_addr='tcp://xf28id2-ca2:60615'
zmq_info_addr='tcp://xf28id2-ca2:60625'
# zmq_control_addr='tcp://localhost:60615'
# zmq_info_addr='tcp://localhost:60625'
RM = REManagerAPI(zmq_control_addr=zmq_control_addr, zmq_info_addr=zmq_info_addr)

import _synthesis_queue_RM as sq
sq.synthesis_queue(
                    syringe_list=syringe_list, 
                    pump_list=pump_list, 
                    set_target_list=set_target_list, 
                    target_vol_list=target_vol_list, 
                    rate_list = infuse_rates, 
                    syringe_mater_list=syringe_mater_list, 
                    precursor_list=precursor_list,
                    mixer=mixer, 
                    resident_t_ratio=resident_t_ratio, 
                    prefix=prefix[1:], 
                    sample=sample, 
                    wash_tube=wash_tube, 
                    name_by_prefix=bool(prefix[0]),  
					num_abs=num_uvvis[0], 
					num_flu=num_uvvis[1],
                    det1_time=num_uvvis[2], 
                    zmq_control_addr=zmq_control_addr, 
					zmq_info_addr=zmq_info_addr, 
                    )

if bool(prefix[0]):
    sample = de._auto_name_sample(infuse_rates, prefix=prefix[1:])
print(f'Sample: {sample}')

rate_label_dic =   {'CsPb':'infusion_rate_CsPb', 
                    'Br':'infusion_rate_Br', 
                    'ZnI':'infusion_rate_I2', 
                    'ZnCl':'infusion_rate_Cl'}

new_points_label = ['infusion_rate_CsPb', 'infusion_rate_Br', 'infusion_rate_I2', 'infusion_rate_Cl']

use_good_bad = True
post_dilute = True
write_agent_data = True
agent_data_path = '/home/xf28id2/Documents/ChengHung/202405_halide_data/with_xray'

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


def print_kafka_messages(beamline_acronym_01, beamline_acronym_02, csv_path=csv_path, 
                         key_height=key_height, height=height, distance=distance, 
                         pump_list=pump_list, sample=sample, precursor_list=precursor_list, 
                         mixer=mixer, dummy_test=dummy_kafka, plqy=PLQY, prefix=prefix, 
                         agent_data_path=agent_data_path, peak_target=peak_target, 
                         rate_label_dic=rate_label_dic):

    print(f"Listening for Kafka messages for\n"
                                            f"     raw data: {beamline_acronym_01}\n"
                                            f"analysis data: {beamline_acronym_02}")
    print(f'Defaul parameters:\n'
          f'                  csv path: {csv_path}\n'
          f'                  key height: {key_height}\n'
          f'                  height: {height}\n'
          f'                  distance: {distance}\n'
          f'{post_dilute = }\n'
          f'{use_good_bad = }\n'
          f'{USE_AGENT_iterate = }\n'
          f'{peak_target = }\n'
          f'{write_agent_data = }\n'
          f'{zmq_control_addr = }')


    global tiled_client, path_0, path_1
    # tiled_client = from_profile("nsls2")["xpd"]["raw"]  ## needs permission
    tiled_client = from_profile("xpd")
    path_0  = csv_path
    path_1 = csv_path + '/good_bad'
    
    try:
        os.mkdir(path_1)
    except FileExistsError:
        pass

    import palettable.colorbrewer.diverging as pld
    palette = pld.RdYlGn_4_r
    cmap = palette.mpl_colormap
    color_idx = np.linspace(0, 1, len(sample))

    # plt.figure()
    # def print_message(name, doc):
    def print_message(consumer, doctype, doc, 
                      bad_data = [], good_data = [], check_abs365 = False, finished = [], 
                      agent_iteration = []):
        name, message = doc
        # print(
        #     f"{datetime.datetime.now().isoformat()} document: {name}\n"
        #     f"document keys: {list(message.keys())}\n"
        #     f"contents: {pprint.pformat(message)}\n"
        # )
        if name == 'start' and ('topic' in doc[1]):
            print(
                f"\n\n\n{datetime.datetime.now().isoformat()} documents {name}\n"
                f"document keys: {list(message.keys())}")
            # time.sleep(5)
             
            if 'uid' in message.keys():
                print(f"uid: {message['uid']}")
            if 'plan_name' in message.keys():
                print(f"plan name: {message['plan_name']}")
            if 'detectors' in message.keys(): 
                print(f"detectors: {message['detectors']}")
            if 'pumps' in message.keys(): 
                print(f"pumps: {message['pumps']}")
            # if 'detectors' in message.keys(): 
            #     print(f"detectors: {message['detectors']}")
            if 'uvvis' in message.keys() and message['plan_name']!='count':
                print(f"uvvis mode:\n"
                      f"           integration time: {message['uvvis'][0]} ms\n"
                      f"           num spectra averaged: {message['uvvis'][1]}\n"
                      f"           buffer capacity: {message['uvvis'][2]}"
                      )
            elif 'uvvis' in message.keys() and message['plan_name']=='count':
                print(f"uvvis mode:\n"
                      f"           spectrum type: {message['uvvis'][0]}\n"
                      f"           integration time: {message['uvvis'][2]} ms\n"
                      f"           num spectra averaged: {message['uvvis'][3]}\n"
                      f"           buffer capacity: {message['uvvis'][4]}"
                      )                
            if 'mixer' in message.keys():
                print(f"mixer: {message['mixer']}")
            if 'sample_type' in message.keys():
                print(f"sample type: {message['sample_type']}")
            
        
        global uid    
        uid = []
        if (name == 'event') and ('topic' in doc[1]):
            # print(f"\n\n\n{datetime.datetime.now().isoformat()} documents {name}\n"
            #       f"contents: {pprint.pformat(message)}"
            # )
            # time.sleep(1)
            global entry, iq_Q, iq_I
            
            # entry, iq_Q, iq_I = [], [], []
            iq_I_uid  = doc[1]['data']['chi_I']
            entry = sandbox_tiled_client[iq_I_uid]
            df = entry.read()
            # print(f'{entry.metadata = }')
            iq_Q = df['chi_Q'].to_numpy()
            iq_I = df['chi_I'].to_numpy()


        global stream_list
        ## Acquisition of xray_uvvis_plan finished but analysis of pdfstream not yet
        ## So just stop queue but not assign uid, stream_list
        if (name == 'stop') and ('scattering' in message['num_events']):
            print('\n*** qsever stop for data export, identification, and fitting ***\n')
            print(f"\n\n\n{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(message)}"
            )
            # inst1 = BInst("queue_stop")
            # RM.item_add(inst1, pos='front')
            ## wait 1 second for databroker to save data
            time.sleep(1)                

        
        ## With taking xray_uvvis_pla, analysis of pdfstream finished
        ## queue should stop before (when acquisition finished)
        ## Obtain raw data uid by reading metadata in sandbox Tiled
        elif (name == 'stop') and ('topic' in doc[1]) and (len(message['num_events'])>0):
            print(f"\n\n\n{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(message)}"
            )
            ## wait 1 second for databroker to save data
            time.sleep(1)
            uid = entry.metadata['run_start']
            stream_list = tiled_client[uid].metadata['summary']['stream_names']

        ## Only take a Uv-Vis, no X-ray data but still do analysis of pdfstream
        ## Stop queue first
        ## Obtain raw data uid in bluesky doc, message            
        elif (name == 'stop') and ('take_a_uvvis' in message['num_events']):
            print('\n*** qsever stop for data export, identification, and fitting ***\n')

            print(f"\n\n\n{datetime.datetime.now().isoformat()} documents {name}\n"
                  f"contents: {pprint.pformat(message)}"
            )
                        
            inst1 = BInst("queue_stop")
            RM.item_add(inst1, pos='front')
            ## wait 1 second for databroker to save data
            time.sleep(1)
            uid = message['run_start']
            stream_list = list(message['num_events'].keys())
            
            
        ## When uid is assigned and type is a string, move to data fitting, calculation
        if (name == 'stop') and (type(uid) is str):
            print(f'\n**** start to export uid: {uid} ****\n')
            print(f'\n**** with stream name in {stream_list} ****\n')

            ## Set good/bad data condictions to the corresponding sample
            try:
                kh = key_height[len(finished)]
                hei = height[len(finished)]
                dis = distance[len(finished)]
            except IndexError:
                kh = key_height[0]
                hei = height[0]
                dis = distance[0]
            
            ## obtain phase fraction & particle size from g(r)
            if 'scattering' in stream_list:
                # Get metadat from stream_name fluorescence for plotting
                qepro_dic, metadata_dic = de.read_qepro_by_stream(
                    uid, stream_name='fluorescence', data_agent='tiled', 
                    beamline_acronym=beamline_acronym_01)
                u = plot_uvvis(qepro_dic, metadata_dic)
                
                if use_sandbox:
                    # iq_df = pd.DataFrame()
                    # iq_df['chi_Q'] = iq_Q
                    # iq_df['chi_I'] = iq_I
                    iq_df = np.asarray([iq_Q, iq_I])
                    # iq_df = iq_fn[0]
                else:
                    pass
                    # iq_df = iq_fn[0]
                
                if iq_to_gr:
                    # Grab metadat from stream_name = fluorescence for naming gr file
                    fn_uid = de._fn_generator(uid, beamline_acronym=beamline_acronym_01)
                    gr_fn = f'{fn_uid}_scattering.gr'
                    # Build pdf config file from a scratch
                    pdfconfig = PDFConfig()
                    pdfconfig.readConfig(cfg_fn)
                    pdfconfig.backgroundfiles = bkg_fn[-1]
                    sqfqgr_path = gp.transform_bkg(pdfconfig, iq_df, output_dir=gr_path, 
                                plot_setting={'marker':'.','color':'green'}, test=True, 
                                gr_fn=gr_fn)    
                    gr_data = sqfqgr_path['gr']
                    
                    ## Remove headers by reading gr_data into pd.Dataframe and save again
                    ## Otherwise, headers will cause trouble in pdffit2
                    gr_df = pd.read_csv(gr_data, skiprows=26, names=['Q', 'I'], sep =' ')
                    gr_df.to_csv(gr_data, index=False, header=False, sep =' ')
                    
                if search_and_match:
                    # gr_data = '/home/xf28id2/Documents/ChengHung/pdffit2_example/CsPbBr3/CsPbBr3.gr'
                    refinery = Refinery(mystery_path=gr_data, results_path=results_path, 
                                criteria={"elements":
                                    {#["Pb","Se"], 
                                    #"$in": ["Cs"], 
                                    "$all": ["Pb"],
                                    }},
                                strict=[],
                                # strict=["Pb", "S"],
                                pdf_calculator_kwargs={
                                    "qmin": 1.0, 
                                    "qmax": 18.0,
                                    "rmin": 2.0,
                                    "rmax": 60.0,
                                    "qdamp": 0.031,
                                    "qbroad": 0.032
                                },)
                    refinery.populate_structures_()
                    refinery.populate_pdfs_()
                    refinery.apply_metrics_()
                    sorted_structures_original = refinery.get_sorted_structures(metric='pearsonr', status='original')
                    cif_id = sorted_structures_original[0].material_id
                    cif_fn = glob.glob(os.path.join(results_path, f'**{cif_id}**.cif'))[0]
                    
                    print(f'\n\n*** After matching, the most correlated strucuture is\n' 
                          f'*** {cif_fn} ***\n\n')
                
                if fitting_pdf:
                    # gr_data = '/home/xf28id2/Documents/ChengHung/pdffit2_example/CsPbBr3/CsPbBr3.gr'
                    gr_df = pd.read_csv(gr_data, names=['Q', 'I'], sep =' ')
                    pf = pc._pdffit2_CsPbX3(gr_data, cif_list, rmax=120, qmax=14, qdamp=0.031, qbroad=0.032, 
                                            fix_APD=True, toler=0.001, return_pf=True)
                    phase_fraction = pf.phase_fractions()['mass']
                    particel_size = []
                    for i in range(pf.num_phases()):
                        pf.setphase(i+1)
                        particel_size.append(pf.getvar(pf.spdiameter))
                    # Grab metadat from stream_name = fluorescence for naming gr file
                    fn_uid = de._fn_generator(uid, beamline_acronym=beamline_acronym_01)
                    fgr_fn = os.path.join(gr_path, f'{fn_uid}_scattering.fgr')
                    pf.save_pdf(1, f'{fgr_fn}')
                    pdf_property={'Br_ratio': phase_fraction[0], 'Br_size':particel_size[0]}
                    gr_fit = np.asarray([pf.getR(), pf.getpdf_fit()])
                else:
                    gr_fit = None
                    pdf_property={'Br_ratio': np.nan, 'Br_size': np.nan}
                
                u.plot_iq_to_gr(iq_df, gr_df.to_numpy().T, gr_fit=gr_fit)
                ## remove 'scattering' from stream_list to avoid redundant work in next for loop
                stream_list.remove('scattering')
            
            ## Export, plotting, fitting, calculate # of good/bad data, add queue item
            for stream_name in stream_list:
                ## Read data from databroker and turn into dic
                qepro_dic, metadata_dic = de.read_qepro_by_stream(
                    uid, stream_name=stream_name, data_agent='tiled', beamline_acronym=beamline_acronym_01)
                sample_type = metadata_dic['sample_type']
                ## Save data in dic into .csv file

                if stream_name == 'take_a_uvvis':
                    saving_path = path_1
                else:
                    saving_path = path_0

                de.dic_to_csv_for_stream(saving_path, qepro_dic, metadata_dic, stream_name=stream_name)
                print(f'\n** export {stream_name} in uid: {uid[0:8]} to ../{os.path.basename(saving_path)} **\n')
                ## Plot data in dic
                u = plot_uvvis(qepro_dic, metadata_dic)
                if len(good_data)==0 and len(bad_data)==0:
                    clear_fig=True
                else:
                    clear_fig=False
                u.plot_data(clear_fig=clear_fig)
                print(f'\n** Plot {stream_name} in uid: {uid[0:8]} complete **\n')
                    
                ## Idenfify good/bad data if it is a fluorescence scan in 'take_a_uvvis'
                if qepro_dic['QEPro_spectrum_type'][0]==2 and stream_name=='take_a_uvvis':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    x0, y0, data_id, peak, prop = da._identify_one_in_kafka(qepro_dic, metadata_dic, key_height=kh, distance=dis, height=hei, dummy_test=dummy_test)
                
                
                ## Apply an offset to zero baseline of absorption spectra
                elif stream_name == 'absorbance':
                    print(f'\n*** start to flter absorbance within 15%-85% due to PF oil phase***\n')
                    ## Apply percnetile filtering for absorption spectra, defaut percent_range = [15, 85]
                    abs_per = da.percentile_abs(qepro_dic['QEPro_x_axis'], qepro_dic['QEPro_output'], percent_range=[15, 85])
                    
                    print(f'\n*** start to check absorbance at 365b nm in stream: {stream_name} is positive or not***\n')
                    # abs_array = qepro_dic['QEPro_output'][1:].mean(axis=0)
                    abs_array = abs_per.mean(axis=0)
                    wavelength = qepro_dic['QEPro_x_axis'][0]

                    popt_abs01, _ = da.fit_line_2D(wavelength, abs_array, da.line_2D, x_range=[205, 240], plot=False)
                    popt_abs02, _ = da.fit_line_2D(wavelength, abs_array, da.line_2D, x_range=[750, 950], plot=False)
                    if abs(popt_abs01[0]) >= abs(popt_abs02[0]):
                        popt_abs = popt_abs02
                    elif abs(popt_abs01[0]) <= abs(popt_abs02[0]):
                        popt_abs = popt_abs01
                    
                    abs_array_offset = abs_array - da.line_2D(wavelength, *popt_abs)

                    print(f'\nFitting function for baseline offset: {da.line_2D}\n')
                    ff_abs={'fit_function': da.line_2D, 'curve_fit': popt_abs, 'percentile_mean': abs_array}
                    de.dic_to_csv_for_stream(saving_path, qepro_dic, metadata_dic, stream_name=stream_name, fitting=ff_abs)
                    u.plot_offfset(wavelength, da.line_2D, popt_abs)
                    print(f'\n** export offset results of absorption spectra complete**\n')

                
                ## Avergae scans in 'fluorescence' and idenfify good/bad
                elif stream_name == 'fluorescence':
                    print(f'\n*** start to identify good/bad data in stream: {stream_name} ***\n')
                    ## Apply percnetile filtering for PL spectra, defaut percent_range = [30, 100]
                    x0, y0, data_id, peak, prop = da._identify_multi_in_kafka(qepro_dic, metadata_dic, 
                                                key_height=kh, distance=dis, height=hei, 
                                                dummy_test=dummy_test, percent_range=[30, 100])
                    label_uid = f'{uid[0:8]}_{metadata_dic["sample_type"]}'
                    # u.plot_average_good(x0, y0, color=cmap(color_idx[sub_idx]), label=label_uid)
                    # sub_idx = sample.index(metadata_dic['sample_type'])
                    u.plot_average_good(x0, y0, label=label_uid, clf_limit=9)
                    
                ## Skip peak fitting if qepro type is absorbance
                if qepro_dic['QEPro_spectrum_type'][0] == 3:  
                    print(f"\n*** No need to carry out fitting for {stream_name} in uid: {uid[:8]} ***\n")

                else: 
                    ## for a good data, type(peak) will be a np.array and type(prop) will be a dic
                    ## fit the good data, export/plotting fitting results
                    ## append data_id into good_data or bad_data for calculate numbers
                    if (type(peak) is np.ndarray) and (type(prop) is dict):
                        x, y, p, f_fit, popt = da._fitting_in_kafka(x0, y0, data_id, peak, prop, dummy_test=dummy_test)    

                        fitted_y = f_fit(x, *popt)
                        r2_idx1, _ = da.find_nearest(x, popt[1] - 3*popt[2])
                        r2_idx2, _ = da.find_nearest(x, popt[1] + 3*popt[2])
                        r_2 = da.r_square(x[r2_idx1:r2_idx2], y[r2_idx1:r2_idx2], fitted_y[r2_idx1:r2_idx2], y_low_limit=0)               

                        metadata_dic["r_2"] = r_2                                   
                        
                        if 'gauss' in f_fit.__name__:
                            constant = 2.355
                        else:
                            constant = 1

                        intensity_list = []
                        peak_list = []
                        fwhm_list = []
                        for i in range(int(len(popt)/3)):
                            intensity_list.append(popt[i*3+0])
                            peak_list.append(popt[i*3+1])
                            fwhm_list.append(popt[i*3+2]*constant)
                        
                        peak_emission_id = np.argmax(np.asarray(intensity_list))
                        peak_emission = peak_list[peak_emission_id]
                        fwhm = fwhm_list[peak_emission_id]

                        ## Calculate PLQY for fluorescence stream
                        if (stream_name == 'fluorescence') and (PLQY[0]==1):
                            PL_integral_s = integrate.simpson(y)
                            label_uid = f'{uid[0:8]}_{metadata_dic["sample_type"]}'
                            u.plot_CsPbX3(x, y, peak_emission, label=label_uid, clf_limit=9)
                            
                            ## Find absorbance at 365 nm from absorbance stream
                            # q_dic, m_dic = de.read_qepro_by_stream(uid, stream_name='absorbance', data_agent='tiled')
                            # abs_array = q_dic['QEPro_output'][1:].mean(axis=0)
                            # wavelength = q_dic['QEPro_x_axis'][0]
                            
                            idx1, _ = da.find_nearest(wavelength, PLQY[2])
                            absorbance_s = abs_array_offset[idx1]

                            if PLQY[1] == 'fluorescein':
                                plqy = da.plqy_fluorescein(absorbance_s, PL_integral_s, 1.506, *PLQY[3:])
                            else:
                                plqy = da.plqy_quinine(absorbance_s, PL_integral_s, 1.506, *PLQY[3:])

                            
                            plqy_dic = {'PL_integral':PL_integral_s, 'Absorbance_365':absorbance_s, 'plqy': plqy}
                            
                            optical_property = {'PL_integral':PL_integral_s, 'Absorbance_365':absorbance_s, 
                                                'Peak': peak_emission, 'FWHM':fwhm, 'PLQY':plqy}


                            ## Creat agent_data in type of dict for exporting as json and wirte to sandbox
                            agent_data = {}
                            agent_data.update(optical_property)
                            agent_data.update(pdf_property)
                            agent_data.update({k:v for k, v in metadata_dic.items() if len(np.atleast_1d(v)) == 1})
                            agent_data = de._exprot_rate_agent(metadata_dic, rate_label_dic, agent_data)

                            ## Update absorbance offset and fluorescence fitting results inot agent_data
                            agent_data.update({'abs_offset':{'fit_function':ff_abs['fit_function'].__name__, 'popt':ff_abs['curve_fit'].tolist()}})
                            agent_data.update({'PL_fitting':{'fit_function':ff['fit_function'].__name__, 'popt':ff['curve_fit'].tolist()}})


                            if USE_AGENT_iterate:

                                # print(f"\ntelling agent {agent_data}")
                                agent = build_agen(peak_target=peak_target, agent_data_path=agent_data_path)

                                if len(agent.table) < 2:
                                    acq_func = "qr"
                                else:
                                    acq_func = "qei"
                                
                                new_points = agent.ask(acq_func, n=1)

                                ## Get target of agent.ask()
                                agent_target = agent.objectives.summary['target'].tolist()
                                
                                ## Get mean and standard deviation of agent.ask()
                                res_values = []
                                for i in new_points_label:
                                    if i in new_points['points'].keys():
                                        res_values.append(new_points['points'][i])
                                x_tensor = torch.tensor(res_value)
                                post = agent.posterior(x_tensor)
                                post_mean = post.mean.tolist()[0]
                                post_stddev = post.stddev.tolist()[0]

                                ## apply np.exp for log-transform objectives
                                if_log = agent.objectives.summary['transform']
                                for j in range(if_log.shape[0]):
                                    if if_log[j] == 'log':
                                        post_mean[j] = np.exp(post_mean[j])
                                        post_stddev[j] = np.exp(post_stddev[j])

                                ## Update target, mean, and standard deviation in agent_data
                                agent_data.update({'agent_target': agent_target})
                                agent_data.update({'posterior_mean': post_mean})
                                agent_data.update({'posterior_stddev': post_stddev})
                                
                                
                                # peak_diff = peak_emission - peak_target
                                peak_diff = False

                                # if (peak_diff <= 3) and (peak_diff >=-3):
                                if peak_diff:
                                    print(f'\nTarget peak: {peak_target} nm vs. Current peak: {peak_emission} nm\n')
                                    print(f'\nReach the target, stop iteration, stop all pumps, and wash the loop.\n')

                                    ### Stop all infusing pumps and wash loop
                                    sq.wash_tube_queue2(pump_list, wash_tube, 'ul/min', 
                                                    zmq_control_addr=zmq_control_addr,
                                                    zmq_info_addr=zmq_info_addr)
                                    
                                    inst1 = BInst("queue_stop")
                                    RM.item_add(inst1, pos='front')
                                    agent_iteration.append(False)
                                
                                else:
                                    agent_iteration.append(True)

                        else:
                            # plqy_dic = None
                            optical_property = None
                        
                    ## Save fitting data
                    print(f'\nFitting function: {f_fit}\n')
                    ff={'fit_function': f_fit, 'curve_fit': popt, 'percentile_mean': y0}
                    de.dic_to_csv_for_stream(saving_path, qepro_dic, metadata_dic, stream_name=stream_name, fitting=ff, plqy_dic=plqy_dic)
                    print(f'\n** export fitting results complete**\n')
                    
                    ## Plot fitting data
                    u.plot_peak_fit(x, y, f_fit, popt, peak=p, fill_between=True)
                    print(f'\n** plot fitting results complete**\n')
                    print(f'{peak = }')
                    print(f'{prop = }')
                    
                    ## Append good/bad idetified results
                    if stream_name == 'take_a_uvvis':
                        good_data.append(data_id)

                    elif (type(peak) == list) and (prop == []):
                        bad_data.append(data_id)
                        print(f"\n*** No need to carry out fitting for {stream_name} in uid: {uid[:8]} ***\n")
                        print(f"\n*** since {stream_name} in uid: {uid[:8]} is a bad data.***\n")
            
                    print('\n*** export, identify good/bad, fitting complete ***\n')
                    
                    try :
                        print(f"\n*** {sample_type} of uid: {uid[:8]} has: ***\n"
                              f"{optical_property = }***\n"
                              f"{pdf_property = }***\n")
                    except (UnboundLocalError):
                        pass

                    ## Save processed data in df and agent_data as metadta in sandbox
                    if write_to_sandbox:
                        df = pd.DataFrame()
                        df['wavelength_nm'] = x0
                        df['absorbance_mean'] = abs_array
                        df['absorbance_offset'] = abs_array_offset
                        df['fluorescence_mean'] = y0
                        df['fluorescence_fitting'] = f_fit(x0, *popt)

                        ## use pd.concat to add various length data together
                        # df_new = pd.concat([df, df_iq, df_gr], ignore_index=False, axis=1)

                        sandbox_tiled_client.write_dataframe(df, metadata=agent_data)
                        uri = sandbox_tiled_client.values()[-1].uri
                        sandbox_uid = uri.split('/')[-1]
                        print(f"\nwrote to Tiled sandbox uid: {sandbox_uid}")

                    ## Save agent_data locally
                    if write_agent_data:
                        agent_data.update({'sandbox_uid': sandbox_uid})                               
                        with open(f"{agent_data_path}/{data_id}.json", "w") as f:
                            json.dump(agent_data, f)

                        print(f"\nwrote to {agent_data_path}")

            print(f'*** Accumulated num of good data: {len(good_data)} ***\n')
            print(f'good_data = {good_data}\n')
            print(f'*** Accumulated num of bad data: {len(bad_data)} ***\n')
            print('########### Events printing division ############\n')
            
            
            ## Depend on # of good/bad data, add items into queue item or stop 
            if stream_name == 'take_a_uvvis' and use_good_bad:     
                if len(bad_data) > 3:
                    print('*** qsever aborted due to too many bad scans, please check setup ***\n')

                    ### Stop all infusing pumps and wash loop
                    sq.wash_tube_queue2(pump_list, wash_tube, 'ul/min', 
                                    zmq_control_addr=zmq_control_addr,
                                    zmq_info_addr=zmq_info_addr)
                    
                    RM.queue_stop()
                    
                elif len(good_data) <= 2 and use_good_bad:
                    print('*** Add another fluorescence and absorption scan to the fron of qsever ***\n')
                    
                    restplan = BPlan('sleep_sec_q', 3)
                    RM.item_add(restplan, pos=0)
                    
                    scanplan = BPlan('take_a_uvvis_csv_q', 
                                    sample_type=metadata_dic['sample_type'], 
                                    spectrum_type='Corrected Sample', 
                                    correction_type='Dark', 
                                    pump_list=pump_list, 
                                    precursor_list=precursor_list, 
                                    mixer=mixer)
                    RM.item_add(scanplan, pos=1)
                    RM.queue_start()

                elif len(good_data) > 2 and use_good_bad:
                    print('*** # of good data is enough so go to the next: bundle plan ***\n')
                    bad_data.clear()
                    good_data.clear()
                    finished.append(metadata_dic['sample_type'])
                    print(f'After event: good_data = {good_data}\n')
                    print(f'After event: finished sample = {finished}\n')

                    RM.queue_start()
            
            ## Add predicted new points from ML agent into qserver
            elif stream_name == 'fluorescence' and USE_AGENT_iterate and agent_iteration[-1]:
                print('*** Add new points from agent to the fron of qsever ***\n')
                print(f'*** New points from agent: {new_points} ***\n')
                
                if post_dilute:
                    set_target_list = [0 for i in range(len(pump_list))]
                    # rate_list = new_points['points'].tolist()[0][:-1] + [new_points['points'].sum()]
                    # rate_list = [rr for rr in new_points['points'].tolist()[0] if rr!=0] + [new_points['points'].sum()]
                    # rate_list = np.asarray(rate_list)
                    rate_list = []
                    for i in new_points_label:
                        for key in new_points['points']:
                            if i == key:
                                rate_list.append(new_points['points'][key][0])
                    rate_list.insert(2, sum(rate_list)/10)
                    rate_list.append(sum(rate_list)*5)
                
                else:
                    # set_target_list = [0 for i in range(new_points['points'].shape[1])]
                    set_target_list = [0 for i in range(len(pump_list))]
                    rate_list = new_points['points']
                    
                sample = de._auto_name_sample(rate_list, prefix=prefix[1:])

                sq.synthesis_queue(
                    syringe_list=syringe_list, 
                    pump_list=pump_list, 
                    set_target_list=set_target_list, 
                    target_vol_list=target_vol_list, 
                    rate_list = rate_list, 
                    syringe_mater_list=syringe_mater_list, 
                    precursor_list=precursor_list,
                    mixer=mixer, 
                    resident_t_ratio=resident_t_ratio, 
                    prefix=prefix[1:], 
                    sample=sample, 
                    wash_tube=wash_tube, 
                    name_by_prefix=bool(prefix[0]),  
					num_abs=num_uvvis[0], 
					num_flu=num_uvvis[1], 
                    is_iteration=True, 
                    zmq_control_addr=zmq_control_addr, 
					zmq_info_addr=zmq_info_addr, 
                    )

                RM.queue_start()
    
            # elif use_good_bad:
            else:
                print('*** Move to next reaction in Queue ***\n')
                time.sleep(2)
                # RM.queue_start()


    kafka_config = _read_bluesky_kafka_config_file(config_file_path="/etc/bluesky/kafka.yml")

    # this consumer should not be in a group with other consumers
    #   so generate a unique consumer group id for it
    unique_group_id = f"echo-{beamline_acronym_01}-{str(uuid.uuid4())[:8]}"

    kafka_consumer = BasicConsumer(
        topics=[f"{beamline_acronym_01}.bluesky.runengine.documents", 
                f"{beamline_acronym_02}.bluesky.runengine.documents"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        group_id=unique_group_id,
        consumer_config=kafka_config["runengine_producer_config"],
        process_message = print_message,
    )

    try:
        kafka_consumer.start_polling(work_during_wait=lambda : plt.pause(.1))
    except KeyboardInterrupt:
        print('\nExiting Kafka consumer')
        return()


if __name__ == "__main__":
    import sys
    print_kafka_messages(sys.argv[1], sys.argv[2])
