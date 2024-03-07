
import os
import json
import glob
import sys
import numpy as np
from tqdm import tqdm
# sys.path.insert(0, "/home/xf28id2/src/blop")

from blop import Agent, DOF, Objective


def build_agen(peak_target=660, peak_tolerance=5):
    # data_path = '/home/xf28id2/data_ZnCl2'
    #data_path = '/home/xf28id2/data'
    agent_data_path = '/home/xf28id2/data_halide'


    if peak_target > 525:
        I_up_limit = 200
        Cl_up_limit = 0

    elif peak_target < 515:
        I_up_limit = 0
        Cl_up_limit = 200

    else:
        I_up_limit = 0
        Cl_up_limit = 0


    # dofs = [
    #     DOF(description="CsPb(oleate)3", name="infusion_rate_CsPb", units="uL/min", search_bounds=(8, 110)),
    #     DOF(description="TOABr", name="infusion_rate_Br", units="uL/min", search_bounds=(50, 200)),
    #     DOF(description="ZnCl2", name="infusion_rate_Cl", units="uL/min", search_bounds=(0, Cl_up_limit)),
    #     DOF(description="ZnI2", name="infusion_rate_I2", units="uL/min", search_bounds=(0, I_up_limit)),
    # ]


    dofs = [
        DOF(description="CsPb(oleate)3", name="infusion_rate_CsPb", units="uL/min", search_bounds=(8, 110)),
        DOF(description="TOABr", name="infusion_rate_Br", units="uL/min", search_bounds=(50, 200)),
        DOF(description="ZnI2", name="infusion_rate_I2", units="uL/min", search_bounds=(0, I_up_limit)), 
        DOF(description="ZnCl2", name="infusion_rate_Cl", units="uL/min", search_bounds=(0, Cl_up_limit)),
    ]  
    
    
    objectives = [
        Objective(description="Peak emission", name="Peak", target=(peak_target-peak_tolerance, peak_target+peak_tolerance), weight=100, max_noise=0.25),
        Objective(description="Peak width", name="FWHM", target="min", log=True, weight=5., max_noise=0.25),
        Objective(description="Quantum yield", name="PLQY", target="max", log=True, weight=1., max_noise=0.25),
    ]




    # objectives = [
    #     Objective(name="Peak emission", key="peak_emission", target=525, units="nm"),
    #     Objective(name="Peak width", key="peak_fwhm", minimize=True, units="nm"),
    #     Objective(name="Quantum yield", key="plqy"),
    # ]

    USE_AGENT = False

    agent = Agent(dofs=dofs, objectives=objectives, db=None, verbose=True)
    #agent.load_data("~/blop/data/init.h5")

    metadata_keys = ["time", "uid", "r_2"]

    init_file = "/home/xf28id2/data_halide/init_240122_01.h5"

    # if os.path.exists(init_file):
    #     agent.load_data(init_file)

    # else:
    filepaths = glob.glob(f"{agent_data_path}/*.json")
    filepaths.sort()
    for fp in tqdm(filepaths):
        with open(fp, "r") as f:
            data = json.load(f)

        r_2_min = 0.85
        try: 
            if data['r_2'] < r_2_min:
                print(f'Skip because "r_2" of {os.path.basename(fp)} is {data["r_2"]:.2f} < {r_2_min}.')
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