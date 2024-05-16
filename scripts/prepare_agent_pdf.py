
import os
import json
import glob
import sys
import numpy as np
import pandas as pd
from tqdm import tqdm
# sys.path.insert(0, "/home/xf28id2/src/blop")

from blop import Agent, DOF, Objective


def build_agen_Cl(peak_target=660, peak_tolerance=5, size_target=6, ):
    # data_path = '/home/xf28id2/data_ZnCl2'
    #data_path = '/home/xf28id2/data'
    agent_data_path = '/home/xf28id2/Documents/ChengHung/pdffit2_test'


    dofs = [
        DOF(description="CsPb(oleate)3", name="infusion_rate_CsPb", units="uL/min", search_domain=(8, 110)),
        DOF(description="TOABr", name="infusion_rate_Br", units="uL/min", search_domain=(50, 200)),
        DOF(description="ZnI2", name="infusion_rate_I2", units="uL/min", search_domain=(0, 200)), 
        DOF(description="ZnCl2", name="infusion_rate_Cl", units="uL/min", search_domain=(0, 200)),
    ]
    

    
    peak_up = peak_target+peak_tolerance
    peak_down = peak_target-peak_tolerance
    
    ratio_up = 1-(510-peak_up)*0.99/110
    ratio_down = 1-(510-peak_down)*0.99/110
    
    objectives = [
        Objective(description="Peak emission", name="Peak", target=(peak_down, peak_up), weight=100, max_noise=0.25),
        Objective(description="Peak width", name="FWHM", target="min", transform="log", weight=5., max_noise=0.25),
        Objective(description="Quantum yield", name="PLQY", target="max", transform="log", weight=1., max_noise=0.25),
        Objective(description="Particle size", name="Br_size", target=(size_target-1.5, size_target+1.5), transform="log", weight=0.1, max_noise=0.25),
        Objective(description="Phase ratio", name="Br_ratio", target=(ratio_down, ratio_up), transform="log", weight=0.1, max_noise=0.25),   
    ]




    # objectives = [
    #     Objective(name="Peak emission", key="peak_emission", target=525, units="nm"),
    #     Objective(name="Peafilepath
    print('Start to buid agent')
    agent = Agent(dofs=dofs, objectives=objectives, db=None, verbose=True)
    
    
    if peak_target > 500:
        agent.dofs.infusion_rate_Cl.deactivate()
        agent.dofs.infusion_rate_Cl.device.put(0)

    elif peak_target < 515:        
        agent.dofs.infusion_rate_I2.deactivate()
        agent.dofs.infusion_rate_I2.device.put(0)

    # else:
    #     agent.dofs.infusion_rate_Cl.deactivate()
    #     agent.dofs.infusion_rate_Cl.device.put(0)
        
    #     agent.dofs.infusion_rate_I2.deactivate()
    #     agent.dofs.infusion_rate_I2.device.put(0)
    
    #agent.load_data("~/blop/data/init.h5")

    metadata_keys = ["time", "uid", "r_2"]



    filepaths = glob.glob(f"{agent_data_path}/*.json")
    filepaths.sort()
    
    # fn = agent_data_path + '/' + 'agent_data_update_quinine_CsPbCl3.csv'
    # df = pd.read_csv(fn, sep=',', index_col=False)
    
    
    # for i in range(len(df['uid'])):
    for fp in tqdm(filepaths):
        with open(fp, "r") as f:
            data = json.load(f)
        # print(data)
        # data = {}
        # for key in df.keys():
        #     data[key] = df[key][i]
            
        r_2_min = 0.05
        try: 
            if data['r_2'] < r_2_min:
                print(f'Skip because "r_2" of {df["uid"][i]} is {data["r_2"]:.2f} < {r_2_min}.')
            else: 
                x = {k:[data[k]] for k in agent.dofs.names}
                y = {k:[data[k]] for k in agent.objectives.names}
                metadata = {k:[data.get(k, None)] for k in metadata_keys}
                agent.tell(x=x, y=y, metadata=metadata, train=False, update_models=False)
        
        except (KeyError):
            print(f'{os.path.basename(fp)} has no "r_2".')


    agent._construct_all_models()
    agent._train_all_models()

    print(f'The target of the emission peak is {peak_target} nm.')

    return agent

    # print(agent.ask("qei", n=1))
    # print(agent.ask("qr", n=36))