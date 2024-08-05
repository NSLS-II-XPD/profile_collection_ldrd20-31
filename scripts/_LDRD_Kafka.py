import os
import glob
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import _data_export as de
from _plot_helper import plot_uvvis
import _data_analysis as da
# import _synthesis_queue_RM as sq
import _pdf_calculator as pc
import _get_pdf as gp

# import torch
# from prepare_agent_pdf import build_agen
from diffpy.pdfgetx import PDFConfig
# from tiled.client import from_uri

# from bluesky_queueserver_api.zmq import REManagerAPI
# from bluesky_queueserver_api import BPlan, BInst



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
            'dummy_pdf', 'write_to_sandbox', 'sandbox_uri', 
            'sandbox_tiled_client', 'tiled_client', 
            'fn_TBD', 
            'entry', 'iq_data', 'stream_list', 'uid', 'uid_catalog', 'uid_pdfstream', 
            'gr_data', 'pdf_property', 'gr_fitting', 
            
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

            ## self.inputs.sandbox_uri[0] is just the uri of sandbox
            ## so, turn uri into client and assign it to self.inputs.sandbox_tiled_client
            if type(self.inputs.sandbox_uri[0]) is str:
                self.inputs.sandbox_tiled_client = from_uri(self.inputs.sandbox_uri[0])


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


    def macro_01_get_iq(self, iq_I_uid):
        """macro to get iq data, used in kafka consumer 
        whiel taking xray_uvvis_plan and analysis of pdfstream finished
       
        This macro will
        0. Reset self.inputs.iq_data as an empty list
        1. Assgin sandbox entry to self.inputs.entry
        2. Append 4 elements into self.inputs.iq_data
            self.inputs.iq_data[0]: chi_Q
            self.inputs.iq_data[1]: chi_I
            self.inputs.iq_data[2]: np.array([chi_Q, chi_I]) 
            self.inputs.iq_data[3]: pd.DataFrame([chi_Q, chi_I])
        3. Reset self.inputs.uid to an empty list

        Args:
            iq_I_uid (str): uid of analysis data, read from doc[1]['data']['chi_I']

        """
        self.inputs.uid_pdfstream.append(iq_I_uid)
        self.inputs.entry = self.inputs.sandbox_tiled_client[iq_I_uid]
        df = self.inputs.entry.read()
        # Before appending I(Q) data, reset self.inputs.iq_data as an empty list
        self.inputs.iq_data = []
        self.inputs.iq_data.append(df['chi_Q'].to_numpy())
        self.inputs.iq_data.append(df['chi_I'].to_numpy())

        iq_array = np.asarray([df['chi_Q'].to_numpy(), df['chi_I'].to_numpy()])
        self.inputs.iq_data.append(iq_array)

        iq_df = pd.DataFrame()
        iq_df['q'] = df['chi_Q'].to_numpy()
        iq_df['I(q)'] = df['chi_I'].to_numpy()
        self.inputs.iq_data.append(iq_df)
        
        ## Reset self.inputs.uid to an empty list
        self.inputs.uid = []



    def macro_02_get_uid(self):
        """macro to get raw data uid, used in kafka consumer
       
        This macro will
        1. Assign raw data uid to self.inputs.uid
        2. Append raw data uid to self.inputs.uid_catalog
        3. Update self.inputs.stream_list
        """
        ## wait 1 second for databroker to save data
        time.sleep(1)
        self.inputs.uid = self.inputs.entry.metadata['run_start']
        self.inputs.uid_catalog.append(self.inputs.uid)
        stream_list = self.inputs.tiled_client[self.inputs.uid].metadata['summary']['stream_names']
        ## Reset self.inputs.stream_list to an empty list
        self.inputs.stream_list = []
        for stream_name in syringe_list:
            self.inputs.stream_list.append(stream_name)



    def macro_03_stop_queue_uid(sefl, RM):
        """macro to stop queue and get raw data uid, used in kafka consumer
        while taking a Uv-Vis, no X-ray data but still do analysis of pdfstream
        
        This macro will
        1. Stop queue
        2. Assign raw data uid to self.inputs.uid
        3. Append raw data uid to self.inputs.uid_catalog
        4. Update self.inputs.stream_list

        Args:
            RM (REManagerAPI): Run Engine Manager API. Defaults to RM.
        """
        inst1 = BInst("queue_stop")
        RM.item_add(inst1, pos='front')
        ## wait 1 second for databroker to save data
        time.sleep(1)
        self.inputs.uid = message['run_start']
        self.inputs.uid_catalog.append(self.inputs.uid)
        stream_list = list(message['num_events'].keys())
        ## Reset self.inputs.stream_list to an empty list
        self.inputs.stream_list = []
        for stream_name in syringe_list:
            self.inputs.stream_list.append(stream_name)



    def macro_04_dummy_pdf(sefl):
        """macro to setup a dummy pdf data for testing, used in kafka consumer
        while self.inputs.dummy_pdf[0] is True

        This macro will
        0. Reset self.inputs.iq_data as an empty list
        1. Read pdf data from self.inputs.iq_fn[-1]
        2. Append 4 elements into self.inputs.iq_data
            self.inputs.iq_data[0]: chi_Q
            self.inputs.iq_data[1]: chi_I
            self.inputs.iq_data[2]: np.array([chi_Q, chi_I]) 
            self.inputs.iq_data[3]: pd.DataFrame([chi_Q, chi_I])
        """
        self.inputs.iq_data = []
        iq_array = pd.read_csv(self.inputs.iq_fn[-1], skiprows=1, names=['q', 'I(q)'], sep=' ').to_numpy().T
        self.inputs.iq_data.append(iq_array[0])
        self.inputs.iq_data.append(iq_array[1])
        self.inputs.iq_data.append(iq_array)
        iq_df = pd.read_csv(self.inputs.iq_fn[-1], skiprows=1, names=['q', 'I(q)'], sep=' ')
        self.inputs.iq_data.append(iq_df)



    def macro_05_iq_to_gr(self, beamline_acronym):
        """macro to condcut data reduction from I(Q) to g(r), used in kafka consumer
        
        This macro will
        1. Generate a filename for g(r) data by using metadata of stream_name == fluorescence
        2. Read pdf config file from self.inputs.cfg_fn[-1]
        3. Read pdf background file from self.inputs.bkg_fn[-1]
        4. Generate s(q), f(q), g(r) data by gp.transform_bkg() and save in self.inputs.iq_to_gr_path[0]
        5. Read saved g(r) into pd.DataFrame and save again to remove the headers
        6. Update g(r) data path and data frame to self.inputs.gr_data
            self.inputs.gr_data[0]: gr_data (path)
            self.inputs.gr_data[1]: gr_df

        Args:
            beamline_acronym (str): catalog name for tiled to access data
        """
        # Grab metadat from stream_name = fluorescence for naming gr file
        fn_uid = de._fn_generator(self.inputs.uid, beamline_acronym=beamline_acronym)
        gr_fn = f'{fn_uid}_scattering.gr'

        ### dummy test, e.g., CsPbBr2
        if self.inputs.dummy_pdf[0]:
            gr_fn = f'{self.inputs.iq_fn[-1][:-4]}.gr'

        # Build pdf config file from a scratch
        pdfconfig = PDFConfig()
        pdfconfig.readConfig(self.inputs.cfg_fn[-1])
        pdfconfig.backgroundfiles = self.inputs.bkg_fn[-1]
        sqfqgr_path = gp.transform_bkg(pdfconfig, self.inputs.iq_array[2], output_dir=self.inputs.iq_to_gr_path[0], 
                    plot_setting={'marker':'.','color':'green'}, test=True, 
                    gr_fn=gr_fn)    
        gr_data = sqfqgr_path['gr']

        ## Remove headers by reading gr_data into pd.Dataframe and save again
        gr_df = pd.read_csv(gr_data, skiprows=26, names=['r', 'g(r)'], sep =' ')
        gr_df.to_csv(gr_data, index=False, header=False, sep =' ')

        self.inputs.gr_data = []
        self.inputs.gr_data.append(gr_data)
        self.inputs.gr_data.append(gr_df)



    def macro_06_search_and_match(self, gr_fn):
        """macro to search and match the best strucutre, used in kafka consumer
        using package Refinery from updated_pipeline_pdffit2.py  

        Args:
            gr_fn (str): g(r) data path for searching and matching, ex: self.inputs.gr_data[0] or self.inputs.gr_fn[0]
                        if using self.inputs.gr_data[0], g(r) is generated in workflow
                        if using self.inputs.gr_fn[0], g(r) is directly read from a file

        Returns:
            str: the file name of the best fitted cif
        """
        from updated_pipeline_pdffit2 import Refinery
        refinery = Refinery(mystery_path=gr_fn, results_path=self.inputs.results_path[0], 
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
        
        return cif_fn



    def macro_07_fitting_pdf(self, gr_fn, beamline_acronym, 
                            rmax=100.0, qmax=12.0, qdamp=0.031, qbroad=0.032, 
                            fix_APD=True, toler=0.01):
        """macro to do pdf fitting by pdffit2 package, used in kafka consumer

        This macro will
        1. Do pdf refinement of gr_fn
        2. Generate a filename for fitting data by using metadata of stream_name == fluorescence
        3. Save fitting data
        4. Update self.inputs.pdf_property
        5. Update fitting data at self.inputs.gr_fitting
            self.inputs.gr_fitting[0]: pf.getR()
            self.inputs.gr_fitting[1]: pf.getpdf_fit()
            self.inputs.gr_fitting[2]: np.array([pf.getR(), pf.getpdf_fit()]) 
            self.inputs.gr_fitting[3]: pd.DataFrame([pf.getR(), pf.getpdf_fit()])

        Args:
            gr_fn (str): g(r) data path for pdf fitting, ex: self.inputs.gr_data[0] or self.inputs.gr_fn[0]
                        if using self.inputs.gr_data[0], g(r) is generated in workflow
                        if using self.inputs.gr_fn[0], g(r) is directly read from a file

            beamline_acronym (str): catalog name for tiled to access data
            rmax (float, optional): pdffit2 variable. Defaults to 100.
            qmax (float, optional): pdffit2 variable. Defaults to 12.
            qdamp (float, optional): pdffit2 variable. Defaults to 0.031.
            qbroad (float, optional): pdffit2 variable. Defaults to 0.032.
            fix_APD (bool, optional): pdffit2 variable. Defaults to True.
            toler (float, optional): pdffit2 variable. Defaults to 0.01.
        """

        pf = pc._pdffit2_CsPbX3(gr_fn, self.inputs.cif_fn, rmax=rmax, qmax=qmax, qdamp=qdamp, qbroad=qbroad, 
                                fix_APD=fix_APD, toler=toler, return_pf=True)
        
        phase_fraction = pf.phase_fractions()['mass']
        particel_size = []
        for i in range(pf.num_phases()):
            pf.setphase(i+1)
            particel_size.append(pf.getvar(pf.spdiameter))
        # Grab metadat from stream_name = fluorescence for naming gr file
        fn_uid = de._fn_generator(self.inputs.uid, beamline_acronym=beamline_acronym)
        fgr_fn = os.path.join(self.inputs.fitting_pdf_path[0], f'{fn_uid}_scattering.fgr')
        pf.save_pdf(1, f'{fgr_fn}')
        
        self.inputs.pdf_property = {}
        self.inputs.pdf_property.update({'Br_ratio': phase_fraction[0], 'Br_size':particel_size[0]})
        
        gr_fit_arrary = np.asarray([pf.getR(), pf.getpdf_fit()])
        gr_fit_df = pd.DataFrame()
        gr_fit_df['fit_r'] = pf.getR()
        gr_fit_df['fit_g(r)'] = pf.getpdf_fit()

        self.inputs.gr_fitting = []
        self.inputs.gr_fitting.append(pf.getR())
        self.inputs.gr_fitting.append(pf.getpdf_fit())
        self.inputs.gr_fitting.append(gr_fit_arrary)
        self.inputs.gr_fitting.append(gr_fit_df)



    def macro_08_no_fitting_pdf(self):
        """macro to update self.inputs.gr_fitting while no pdf fitting, used in kafka consumer
        """
        self.inputs.gr_fitting = []
        gr_fit_arrary = None
        
        gr_fit_df = pd.DataFrame()
        gr_fit_df['fit_r'] = np.nan
        gr_fit_df['fit_g(r)'] = np.nan
        
        self.inputs.gr_fitting.append([])
        self.inputs.gr_fitting.append([])
        self.inputs.gr_fitting.append(gr_fit_arrary)
        self.inputs.gr_fitting.append(gr_fit_df)
        
        pdf_property={'Br_ratio': np.nan, 'Br_size': np.nan}
        self.inputs.pdf_property.update(pdf_property)



                
