
import os
import json
import glob
import sys
import numpy as np
from tqdm import tqdm
sys.path.insert(0, "/home/xf28id2/src/bloptools")

from bloptools.bayesian import Agent, DOF, Objective

# data_path = '/home/xf28id2/data_ZnCl2'
#data_path = '/home/xf28id2/data'
agent_data_path = '/home/xf28id2/data_halide'

dofs = [
    DOF(description="CsPb(oleate)3", name="infusion_rate_CsPb", units="uL/min", limits=(10, 110)),
    DOF(description="TOABr", name="infusion_rate_Br", units="uL/min", limits=(70, 170)),
    DOF(description="ZnCl2", name="infusion_rate_Cl", units="uL/min", limits=(0, 110)),
    DOF(description="ZnI2", name="infusion_rate_I2", units="uL/min", limits=(0, 110)),
]

objectives = [
    Objective(description="Peak emission", name="Peak", target=640, weight=10, min_snr=2),
    Objective(description="Peak width", name="FWHM", target="min", log=True, weight=2., min_snr=2),
    Objective(description="Quantum yield", name="PLQY", target="max", log=True, weight=1., min_snr=2),
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

filepaths = glob.glob(f"{agent_data_path}/*.json")
for fp in tqdm(filepaths):
    with open(fp, "r") as f:
        data = json.load(f)

    r_2_min = 0.92
    try: 
        if data['r_2'] < r_2_min:
            print(f'Skip because "r_2" of {os.path.basename(fp)} is {data["r_2"]:.2f} < {r_2_min}.')
        else: 
            x = {k:[data[k]] for k in agent.dofs.names}
            y = {k:[data[k]] for k in agent.objectives.names}
            metadata = {k:[data.get(k, None)] for k in metadata_keys}
            agent.tell(x=x, y=y, metadata=metadata)
    
    except (KeyError):
        print(f'{os.path.basename(fp)} has no "r_2".')

agent._construct_models()

# print(agent.ask("qei", n=1))
# print(agent.ask("qr", n=36))