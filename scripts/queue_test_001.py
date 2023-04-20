from bluesky_queueserver.manager.comms import zmq_single_request

'''
pump, syringe, precursor, mixer parameters
'''
# pump_list = [dds1_p1.name, dds1_p2.name]
pump_list = ['dds1_p1', 'dds1_p2']
syringe_list = [50, 50]
target_vol_list = ['30 ml', '30 ml']
infuse_rates = [['100 ul/min', '100 ul/min'], ['200 ul/min', '200 ul/min'], ['50 ul/min', '50 ul/min']]
precursor_list = ['CsPbOA', 'ToABr']
mixer = ['30 cm']
syringe_mater_list=['steel', 'steel']
sample = ['CsPbBr_100ul', 'CsPbBr_200ul', 'CsPbBr_50ul']


zmq_single_request(method='status')
zmq_single_request(method='queue_get')

'''
Arrange tasks in queue
'''
i=0
## 0. Clear queue
zmq_single_request(method='queue_clear')

for i in range(len(infuse_rates)):
# for i in range(2): 
    ## 1. reset pump
    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"reset_pumps2", 
                                    "args": [pump_list], 
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})


    ## 2. set i infuese rates
    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"set_group_infuse", 
                                    "args": [syringe_list, pump_list], 
                                    "kwargs": {"rate_list":infuse_rates[i], "target_vol_list":target_vol_list}, 
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})


    ## 3. start infuese
    # zmq_single_request(method='queue_item_add', 
    #                 params={
    #                         'item':{"name":"start_group_infuse", 
    #                                 "args": [pump_list],  
    #                                 "item_type":"plan"
    #                                 }, 'user_group':'primary', 'user':'chlin'})


    ## 4. wait for equilibrium
    # zmq_single_request(method='queue_item_add', 
    #                 params={
    #                         'item':{"name":"wait_equilibrium", 
    #                                 "args": [pump_list, mixer], 
    #                                 "kwargs": {"ratio":1}, 
    #                                 "item_type":"plan"
    #                                 }, 'user_group':'primary', 'user':'chlin'})

    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"sleep_sec_q", 
                                    "args":[2], 
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})    


    ## 5. take a fluorescence peak to check reaction
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

    ## 6. Sleep for 5 seconds for Kafak to check good/bad data
    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"sleep_sec_q", 
                                    "args":[5], 
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})
    
    
    
    
    
    ## 7. start xray_uvvis bundle plan to take real data
    zmq_single_request(method='queue_item_add', 
                    params={
                            'item':{"name":"xray_uvvis_plan", 
                                    "args":['det', 'qepro'],
                                    "kwargs": {'num_abs':1, 'num_flu':2,
                                                'sample_type':sample[i], 
                                                'spectrum_type':'Absorbtion', 'correction_type':'Reference', 
                                                'pump_list':pump_list, 'precursor_list':precursor_list, 
                                                'mixer':mixer}, 
                                    "item_type":"plan"
                                    }, 'user_group':'primary', 'user':'chlin'})

    #### Kafka analyze data here.

    ## Wash the loop and mixer?
    



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




