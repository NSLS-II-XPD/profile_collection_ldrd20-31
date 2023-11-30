import numpy as np
import pandas as pd
from bluesky_queueserver.manager.comms import zmq_single_request
import _data_export as de

'''
pump, syringe, precursor, mixer parameters
'''
## Input varaibales: read from inputs_qserver_kafka.xlsx
# xlsx = '/home/xf28id2/Documents/ChengHung/inputs_qserver_kafka_ML.xlsx'
# input_dic = de._read_input_xlsx(xlsx)

# ##################################################################
# # Define namespace for tasks in Qserver and Kafa
# dummy_kafka = bool(input_dic['dummy_test'][0])
# dummy_qserver = bool(input_dic['dummy_test'][1])
# csv_path = input_dic['csv_path'][0]
# key_height = input_dic['key_height']
# height = input_dic['height']
# distance = input_dic['distance']
# pump_list = input_dic['pump_list']
# precursor_list = input_dic['precursor_list']
# syringe_mater_list = input_dic['syringe_mater_list']
# syringe_list = input_dic['syringe_list']
# target_vol_list = input_dic['target_vol_list']
# set_target_list = input_dic['set_target_list']
# infuse_rates = input_dic['infuse_rates']
# sample = input_dic['sample']
# mixer = input_dic['mixer']
# wash_tube = input_dic['wash_tube']
# resident_t_ratio = input_dic['resident_t_ratio'][0]
# PLQY = input_dic['PLQY']
# prefix = input_dic['prefix']
# ###################################################################


'''
def set_group_infuse2(
                      syringe_list, 
                      pump_list, 
                      set_target_list=[True, True], 
                      target_vol_list=['50 ml', '50 ml'], 
                      rate_list = ['100 ul/min', '100 ul/min'], 
                      syringe_mater_list=['steel', 'steel']
                      ):
'''


## Add ietration of ML agent into Qsever
def iterate_queue(
                new_points, 
                syringe_list, 
                pump_list, 
                target_vol_list, 
                rate_list, 
                syringe_mater_list, 
                precursor_list,
                mixer, 
                resident_t_ratio, 
                prefix, 
                wash_tube, 
                rate_unit='ul/min', 
                dummy_qserver=False, 
                ):
        
        # To do: unit conversion
        # if rate_unit != 'ul/min':
        
        # num_iteration = new_points[0].shape[0]
        set_target_list = [0 for i in range(new_points[0].shape[1])]
        infuse_rates_float = new_points[0]

        unit_array = np.full([infuse_rates.shape[0], infuse_rates.shape[1]],' ul/min', dtype='U7')
        infuse_rates_string = infuse_rates.astype('U25')
        infuse_rates = np.char.add(infuse_rates_string, unit_array)

        sample = de._auto_name_sample(new_points[0], prefix=prefix):
        def synthesis_queue(
                    syringe_list=syringe_list, 
                    pump_list=pump_list, 
                    set_target_list=set_target_list, 
                    target_vol_list=target_vol_list, 
                    rate_list=infuse_rates, 
                    syringe_mater_list=syringe_mater_list, 
                    precursor_list=precursor_list,
                    mixer=mixer, 
                    resident_t_ratio=resident_t_ratio, 
                    prefix=prefix, 
                    sample=sample, 
                    wash_tube=wash_tube, 
                    dummy_qserver=False, 
                    )


## Arrange tasks of for PQDs synthesis
def synthesis_queue(
                    syringe_list, 
                    pump_list, 
                    set_target_list, 
                    target_vol_list, 
                    rate_list, 
                    syringe_mater_list, 
                    precursor_list,
                    mixer, 
                    resident_t_ratio, 
                    prefix, 
                    sample, 
                    wash_tube, 
                    dummy_qserver=False, 
                    ):
        for i in range(len(rate_list)):
                # for i in range(2): 
                ## 1. Set i infuese rates
                for sl, pl, ir, tvl, stl, sml in zip(syringe_list, pump_list, rate_list[i], target_vol_list, set_target_list[i], syringe_mater_list):
                        zmq_single_request(method='queue_item_add', 
                                        params={
                                                'item':{"name":"set_group_infuse2", 
                                                        "args": [[sl], [pl]], 
                                                        "kwargs": {"rate_list":[ir], "target_vol_list":[tvl], "set_target_list":[stl], "syringe_mater_list":[sml]}, 
                                                        "item_type":"plan"
                                                        }, 'user_group':'primary', 'user':'chlin'})


                ## 2. Start infuese
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"start_group_infuse", 
                                                "args": [pump_list, rate_list[i]],  
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})


                ## 3. Wait for equilibrium
                if dummy_qserver:
                        zmq_single_request(method='queue_item_add', 
                                        params={
                                                'item':{"name":"sleep_sec_q", 
                                                        "args":[5], 
                                                        "item_type":"plan"
                                                        }, 'user_group':'primary', 'user':'chlin'}) 
                
                else:
                        zmq_single_request(method='queue_item_add', 
                                        params={
                                                'item':{"name":"wait_equilibrium", 
                                                        "args": [pump_list, mixer], 
                                                        "kwargs": {"ratio":resident_t_ratio}, 
                                                        "item_type":"plan"
                                                        }, 'user_group':'primary', 'user':'chlin'})

                


                ## 4. Take a fluorescence peak to check reaction
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"take_a_uvvis_csv_q",  
                                                "kwargs": {'sample_type':sample[i], 
                                                                'spectrum_type':'Corrected Sample', 'correction_type':'Dark', 
                                                                'pump_list':pump_list, 'precursor_list':precursor_list, 
                                                                'mixer':mixer}, 
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})

                #### Kafka check data here.

                ## 5. Sleep for 5 seconds for Kafak to check good/bad data
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"sleep_sec_q", 
                                                "args":[2], 
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})
                
                
                
                
                
                ## 6. Start xray_uvvis bundle plan to take real data
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"xray_uvvis_plan", 
                                                "args":['det', 'qepro'],
                                                "kwargs": {'num_abs':9, 'num_flu':9,
                                                                'sample_type':sample[i], 
                                                                'spectrum_type':'Absorbtion', 'correction_type':'Reference', 
                                                                'pump_list':pump_list, 'precursor_list':precursor_list, 
                                                                'mixer':mixer}, 
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})

                
                
                ######  Kafka analyze data here. #######

                
                ## 7. Wash the loop and mixer
                ### 7-1. Stop infuese
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"stop_group", 
                                                "args": [pump_list],  
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})

                
                ### 7-2. Set 100 ul/min at ZnI2/ZnCl2
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"set_group_infuse2", 
                                                "args": [[wash_tube[0]], [wash_tube[1]]], 
                                                "kwargs": {"rate_list":[wash_tube[2]], "target_vol_list":['30 ml'], "set_target_list":[False]}, 
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})


                ### 7-3. Start to infuse ZnI2/ZnCl2 to wash loop/tube
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"start_group_infuse", 
                                                "args": [[wash_tube[1]], [wash_tube[2]]],  
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})


                ### 7-4. Wash loop/tube for 300 seconds
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"sleep_sec_q", 
                                                "args":[wash_tube[3]], 
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})


                ### 7-5. stop infuese
                zmq_single_request(method='queue_item_add', 
                                params={
                                        'item':{"name":"stop_group", 
                                                "args": [[wash_tube[1]]],  
                                                "item_type":"plan"
                                                }, 'user_group':'primary', 'user':'chlin'})




        # 8. stop infuese for all pumps
        zmq_single_request(method='queue_item_add', 
                        params={
                                'item':{"name":"stop_group", 
                                        "args": [pump_list],  
                                        "item_type":"plan"
                                        }, 'user_group':'primary', 'user':'chlin'})


'''
 'plans_allowed': {'count': '{...}',
                   'scan': '{...}',
                   'LED_on': '{...}',
                   'LED_off': '{...}',
                   'shutter_open': '{...}',
                   'shutter_close': '{...}',
                   'deuterium_on': '{...}',
                   'deuterium_off': '{...}',
                   'halogen_on': '{...}',
                   'halogen_off': '{...}',
                   'reset_pumps2': '{...}',
                   'set_group_infuse': '{...}',
                   'set_group_withdraw': '{...}',
                   'start_group_infuse': '{...}',
                   'start_group_withdraw': '{...}',
                   'stop_group': '{...}',
                   'insitu_test': '{...}',
                   'setup_collection_q': '{...}',
                   'take_ref_bkg_q': '{...}',
                   'take_a_uvvis_csv_q': '{...}',
                   'sleep_sec_q': '{...}',
                   'wait_equilibrium': '{...}',
                   'xray_uvvis_plan': '{...}'},

'''
