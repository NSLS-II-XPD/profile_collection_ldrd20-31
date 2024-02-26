from bluesky_queueserver.manager.comms import zmq_single_request
import _data_export as de

'''
pump, syringe, precursor, mixer parameters
'''
## Input varaibales: read from inputs_qserver_kafka.xlsx
xlsx = '/home/xf28id2/Documents/ChengHung/inputs_qserver_kafka.xlsx'
input_dic = de._read_input_xlsx(xlsx)

##################################################################
# Define namespace for tasks in Qserver and Kafa
dummy_test = input_dic['dummy_test'][0]
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
resident_t_ratio = input_dic['resident_t_ratio'][0]
###################################################################


zmq_single_request(method='status')
zmq_single_request(method='queue_get')
zmq_single_request(method='queue_clear')

'''
Arrange tasks in queue
'''
i=0
## 0. Clear queue and reset syringe pumps
zmq_single_request(method='queue_clear')

# zmq_single_request(method='queue_item_add', 
#                 params={
#                         'item':{"name":"reset_pumps2", 
#                                 "args": [pump_list], 
#                                 "item_type":"plan"
#                                 }, 'user_group':'primary', 'user':'chlin'})

for i in range(len(infuse_rates)):
# for i in range(2): 
    ## 1. Set i infuese rates
    for sl, pl, ir, tvl, stl in zip(syringe_list, pump_list, infuse_rates[i], target_vol_list, set_target_list[i]):
        zmq_single_request(method='queue_item_add', 
                        params={
                                'item':{"name":"set_group_infuse2", 
                                        "args": [[sl], [pl]], 
                                        "kwargs": {"rate_list":[ir], "target_vol_list":[tvl], "set_target_list":[stl]}, 
                                        "item_type":"plan"
                                        }, 'user_group':'primary', 'user':'chlin'})


    ## 2. Start infuese
    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"start_group_infuse", 
                                    "args": [pump_list],  
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})


    ## 3. Wait for equilibrium
    if dummy_test:
        zmq_single_request(method='queue_item_add', 
                           params={
                                   'item':{"name":"sleep_sec_q", 
                                            "args":[30], 
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
                                    "kwargs": {'num_abs':16, 'num_flu':16,
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
                                "args": [[wash_tube[1]]],  
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




