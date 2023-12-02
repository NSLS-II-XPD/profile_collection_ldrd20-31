import numpy as np
import pandas as pd
from bluesky_queueserver.manager.comms import zmq_single_request
import _data_export as de

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
                    ):

	if name_by_prefix:
		sample = _auto_name_sample(rate_list, prefix=prefix)
	                                                                                 
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
			params={
				'item':{
					"name":"start_group_infuse", 
					"args": [pump_list, rate_list[i]],  
					"item_type":"plan"}, 
				'pos': pos, 
				'user_group':'primary', 
				'user':'chlin'})


		## 3. Wait for equilibrium
		if dummy_qserver:
			zmq_single_request(
				method='queue_item_add', 
				params={
					'item':{
						"name":"sleep_sec_q", 
						"args":[5], 
						"item_type":"plan"}, 
					'pos': pos, 
					'user_group':'primary', 
					'user':'chlin'}) 
		
		else:
			zmq_single_request(
				method='queue_item_add', 
				params={
					'item':{
						"name":"wait_equilibrium", 
						"args": [pump_list, mixer], 
						"kwargs": {"ratio":resident_t_ratio}, 
						"item_type":"plan"}, 
					'pos': pos, 
					'user_group':'primary', 
					'user':'chlin'})

		

		## 4. Take a fluorescence peak to check reaction
		zmq_single_request(
			method='queue_item_add', 
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

		#### Kafka check data here.

		## 5. Sleep for 5 seconds for Kafak to check good/bad data
		zmq_single_request(
			method='queue_item_add', 
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
			params={
				'item':{
					"name":"xray_uvvis_plan", 
					"args":['det', 'qepro'],
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
def _auto_name_sample(infuse_rates, prefix=None):
    infuse_rates = np.asarray(infuse_rates)

    if len(infuse_rates.shape) == 1:
        infuse_rates = infuse_rates.reshape(1, infuse_rates.shape[0])

    if prefix == None:
        prefix_list = [f'Pre{i:02d}' for i in range(infuse_rates.shape[1])]
    else:
        prefix_list = prefix

    sample = []
    for i in range(infuse_rates.shape[0]):
        name = ''
        for j in range(infuse_rates.shape[1]):
            int_rate = int(round(float(infuse_rates[i][j]), 0))
            name += f'{prefix_list[j]}_{int_rate:03d}_'
        sample.append(name[:-1])
    
    return sample





def vol_unit_converter(v0 = 'ul', v1 = 'ml'):
    vol_unit = ['pl', 'nl', 'ul', 'ml']
    vol_frame = pd.DataFrame(data={'pl': np.geomspace(1, 1E9, num=4), 'nl': np.geomspace(1E-3, 1E6, num=4),
                                   'ul': np.geomspace(1E-6, 1E3, num=4), 'ml': np.geomspace(1E-9, 1, num=4)}, index=vol_unit)
    return vol_frame.loc[v0, v1]


def t_unit_converter(t0 = 'min', t1 = 'min'):
    t_unit = ['sec', 'min', 'hr']
    t_frame = pd.DataFrame(data={'sec': np.geomspace(1, 3600, num=3), 
                                 'min': np.geomspace(1/60, 60, num=3), 
                                 'hr' : np.geomspace(1/3600, 1, num=3)}, index=t_unit)
    return t_frame.loc[t0, t1]


def rate_unit_converter(r0 = 'ul/min', r1 = 'ul/min'):
    
    v0 = r0.split('/')[0]
    t0 = r0.split('/')[1]
    v1 = r1.split('/')[0]
    t1 = r1.split('/')[1]

    ## ruc = rate_unit_converter
    ruc = vol_unit_converter(v0=v0, v1=v1) / t_unit_converter(t0=t0, t1=t1)
    return ruc





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
