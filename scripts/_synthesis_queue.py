import numpy as np
import pandas as pd
from bluesky_queueserver.manager.comms import zmq_single_request
import _data_export as de

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
					rate_unit='ul/min',
					name_by_prefix=True,  
					num_abs=5, 
					num_flu=5, 
					pos='back',
                    dummy_qserver=False,
					is_iteration=False, 
					zmq_server_address=None, 
                    ):

	if name_by_prefix:
		sample = de._auto_name_sample(rate_list, prefix=prefix)
	                                                                                 
	rate_list = np.asarray(rate_list, dtype=np.float32)
	if len(rate_list.shape) == 1:
		rate_list = rate_list.reshape(1, rate_list.shape[0])
		rate_list = rate_list.tolist()
	else:
		rate_list = rate_list.tolist()

	set_target_list = np.asarray(set_target_list, dtype=np.int8)
	if len(set_target_list.shape) == 1:
		set_target_list = set_target_list.reshape(1, set_target_list.shape[0])
		set_target_list = set_target_list.tolist()
	else:
		set_target_list = set_target_list.tolist()
		

	for i in range(len(rate_list)):
		# for i in range(2): 
		## 1. Set i infuese rates
		for sl, pl, ir, tvl, stl, sml in zip(
											syringe_list, 
											pump_list, 
											rate_list[i], 
											target_vol_list, 
											set_target_list[i], 
											syringe_mater_list
											):
			
			# ir = float(ir)
			# stl = int(stl)

			zmq_single_request(
				method='queue_item_add',
				zmq_server_address=zmq_server_address, 
				params={
					'item':{
						"name":"set_group_infuse2", 
						"args": [[sl], [pl]], 
						"kwargs": {
							"rate_list":[ir], 
							"target_vol_list":[tvl], 
							"set_target_list":[stl], 
							"syringe_mater_list":[sml], 
							"rate_unit":rate_unit}, 
						"item_type":"plan"}, 
					'pos': pos, 
					'user_group':'primary', 
					'user':'chlin'})


		## 2. Start infuese
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"start_group_infuse", 
					"args": [pump_list, rate_list[i]],  
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})


		## 3. Wait for equilibrium
		
		# mixer_pump_list = []
		# for i in range(len(mixer)):
		# 	mixer_pump = [mixer[i], pump]
		if len(mixer) == 1:
			mixer_pump_list = [[mixer[0], *pump_list]]
		elif len(mixer) == 2:
			mixer_pump_list = [[mixer[0], *pump_list[:3]], [mixer[1], *pump_list]]
		
		if dummy_qserver:
			zmq_single_request(
				method='queue_item_add', 
				zmq_server_address=zmq_server_address, 
				params={
					'item':{
						"name":"sleep_sec_q", 
						"args":[5], 
						"item_type":"plan"}, 
					'pos': pos, 
					'user_group':'primary', 
					'user':'chlin'}) 
		
		else:
			if is_iteration:
				rest_time = resident_t_ratio[-1]
			
			elif len(resident_t_ratio) == 1:
				rest_time = resident_t_ratio[0]
			elif len(resident_t_ratio) > 1 and i==0:
				rest_time = resident_t_ratio[0]
			elif len(resident_t_ratio) > 1 and i>0:
				rest_time = resident_t_ratio[-1]

			zmq_single_request(
				method='queue_item_add', 
				zmq_server_address=zmq_server_address, 
				params={
					'item':{
						"name":"wait_equilibrium2", 
						"args": [mixer_pump_list], 
						"kwargs": {"ratio":rest_time}, 
						"item_type":"plan"}, 
					'pos': pos, 
					'user_group':'primary', 
					'user':'chlin'})

		

		## 4-1. Take a fluorescence peak to check reaction
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"take_a_uvvis_csv_q",  
					"kwargs": {
						'sample_type':sample[i], 
						'spectrum_type':'Corrected Sample', 
						'correction_type':'Dark', 
						'pump_list':pump_list, 
						'precursor_list':precursor_list, 
						'mixer':mixer}, 
					"item_type":"plan"},
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})

		# ## 4-2. Take a Absorption spectra to check reaction
		# zmq_single_request(
		# 	method='queue_item_add', 
		# 	params={
		# 		'item':{
		# 			"name":"take_a_uvvis_csv_q",  
		# 			"kwargs": {
		# 				'sample_type':sample[i], 
		# 				'spectrum_type':'Absorbtion', 
		# 				'correction_type':'Reference', 
		# 				'pump_list':pump_list, 
		# 				'precursor_list':precursor_list, 
		# 				'mixer':mixer}, 
		# 			"item_type":"plan"},
		# 		'pos': pos, 
		# 		'user_group':'primary', 
		# 		'user':'chlin'})

		#### Kafka check data here.

		## 5. Sleep for 5 seconds for Kafak to check good/bad data
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"sleep_sec_q", 
					"args":[2], 
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})
		
		
		
		
		
		## 6. Start xray_uvvis bundle plan to take real data
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"xray_uvvis_plan", 
					"args":['pe2c', 'qepro'],
					"kwargs":{
						'num_abs':num_abs, 
						'num_flu':num_flu,
						'sample_type':sample[i], 
						'spectrum_type':'Absorbtion', 
						'correction_type':'Reference', 
						'pump_list':pump_list, 
						'precursor_list':precursor_list, 
						'mixer':mixer}, 
					"item_type":"plan"}, 
				'pos': pos,
				'user_group':'primary', 
				'user':'chlin'})

		
		
		######  Kafka analyze data here. #######

		
		## 7. Wash the loop and mixer
		### 7-1. Stop infuese
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"stop_group", 
					"args": [pump_list],  
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})

		
		### 7-2. Set infuse rate for washing loop/tube
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"set_group_infuse2", 
					"args": [[wash_tube[0]], [wash_tube[1]]], 
					"kwargs": {
						"rate_list":[wash_tube[2]], 
						"target_vol_list":['30 ml'], 
						"set_target_list":[False], 
						"rate_unit":rate_unit}, 
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})


		### 7-3. Start to wash loop/tube
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"start_group_infuse", 
					"args": [[wash_tube[1]], [wash_tube[2]]],  
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})


		### 7-4. Wash loop/tube for xxx seconds
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"sleep_sec_q", 
					"args":[wash_tube[3]], 
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})


		### 7-5. stop infuese
		zmq_single_request(
			method='queue_item_add', 
			zmq_server_address=zmq_server_address, 
			params={
				'item':{
					"name":"stop_group", 
					"args": [[wash_tube[1]]],  
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})



	# 8. stop infuese for all pumps
	zmq_single_request(
		method='queue_item_add', 
		zmq_server_address=zmq_server_address, 
		params={
			'item':{
				"name":"stop_group", 
				"args": [pump_list],  
				"item_type":"plan"}, 
			'pos': pos, 
			'user_group':'primary', 
			'user':'chlin'})




## Auto generate sample name with given prefix and infuse_rate
## If prefix = None, 'Pre00', 'Pre01', 'Pre02', ... will be used.
# def _auto_name_sample(infuse_rates, prefix=None):
#     infuse_rates = np.asarray(infuse_rates)

#     if len(infuse_rates.shape) == 1:
#         infuse_rates = infuse_rates.reshape(1, infuse_rates.shape[0])

#     if prefix == None:
#         prefix_list = [f'Pre{i:02d}' for i in range(infuse_rates.shape[1])]
#     else:
#         prefix_list = prefix

#     sample = []
#     for i in range(infuse_rates.shape[0]):
#         name = ''
#         for j in range(infuse_rates.shape[1]):
#             int_rate = int(round(float(infuse_rates[i][j]), 0))
#             name += f'{prefix_list[j]}_{int_rate:03d}_'
#         sample.append(name[:-1])
    
#     return sample





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


## Add ietration of ML agent into Qsever
def iterate_queue(
				new_points, 
                syringe_list, 
                pump_list, 
                target_vol_list, 
                syringe_mater_list, 
                precursor_list,
                mixer, 
                resident_t_ratio, 
                prefix, 
                wash_tube, 
				name_by_prefix=True, 
				num_abs=5,
				num_flu=5,
                new_points_unit='ul/min', 
				pos='back', 
                dummy_qserver=False, 
                ):
	        
	# To do: unit conversion
	# if rate_unit != 'ul/min':
	
	# num_iteration = new_points[0].shape[0]
	set_target_list = [0 for i in range(new_points[0].shape[1])]
	sample = _auto_name_sample(new_points[0], prefix=prefix)

	# infuse_rates_float = new_points[0]
	# unit_array = np.full([infuse_rates_float.shape[0], infuse_rates_float.shape[1]],' ul/min', dtype='U7')
	# infuse_rates_string = infuse_rates_float.astype('U25')
	# infuse_rates = np.char.add(infuse_rates_string, unit_array)
	

	return synthesis_queue(
						syringe_list=syringe_list, 
						pump_list=pump_list, 
						set_target_list=set_target_list, 
						target_vol_list=target_vol_list, 
						rate_list=new_points[0], 
						syringe_mater_list=syringe_mater_list, 
						precursor_list=precursor_list,
						mixer=mixer, 
						resident_t_ratio=resident_t_ratio, 
						prefix=prefix, 
						sample=sample, 
						wash_tube=wash_tube, 
						name_by_prefix=name_by_prefix, 
						num_abs=num_abs,
						num_flu=num_flu,
						rate_unit=new_points_unit, 
						pos='back', 
						dummy_qserver=False, 
						)

'''


