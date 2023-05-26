from bluesky_queueserver.manager.comms import zmq_single_request

'''
pump, syringe, precursor, mixer parameters
'''
# pump_list = [dds1_p1.name, dds1_p2.name]
csv_path = '/home/xf28id2/Documents/ChengHung/20230526_CsPbBr_ZnCl_6mM'
key_height = 100
height = 50
distance = 70
# pump_list = [dds1_p1.name, dds1_p2.name]
pump_list = ['dds2_p1', 'dds2_p2', 'dds1_p2']
precursor_list = ['CsPbOA_6mM_20221025', 'TOABr_12mM_20220712', 'ZnCl2_6mM_20220504']
syringe_mater_list=['steel', 'steel', 'steel']
syringe_list = [50, 50, 50]
target_vol_list = ['30 ml', '30 ml', '30 ml']
set_target_list=[
                 [False, False, False],
                 [False, False, False], 
                 ]
infuse_rates = [   
                ['100 ul/min', '100 ul/min', '64 ul/min'], 
                ['100 ul/min', '100 ul/min', '128 ul/min'], 
                ]
sample = [
          'CsPbI3_064ul', 'CsPbI3_128ul', 
          ]

mixer = ['30 cm', '60 cm']
wash_tube = [50, 'dds1_p2', '250 ul/min', 300]  ## [syringe, pump, rate, wash time]
resident_t_ratio = 4
dummy_test = False


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
                                    "args":[10], 
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




