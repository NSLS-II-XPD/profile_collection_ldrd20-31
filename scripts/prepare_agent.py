

import json
import glob
import sys
import numpy as np
sys.path.insert(0, "/home/xf28id2/src/bloptools")

from bloptools.bayesian import Agent, DOF, Objective

data_path = '/home/xf28id2/data_ZnCl2'
#data_path = '/home/xf28id2/data'

dofs = [
    DOF(description="CsPb(oleate)3", name="infusion_rate_1", units="uL/min", limits=(10, 110)),
    DOF(description="TOABr", name="infusion_rate_2", units="uL/min", limits=(70, 170)),
    DOF(description="ZnCl2", name="infusion_rate_3", units="uL/min", limits=(10, 170)),
]

objectives = [
    Objective(description="Peak emission", name="Peak", target=490, weight=10, min_snr=2),
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

filepaths = glob.glob(f"{data_path}/*.json")
for fp in np.array(filepaths):
    with open(fp, "r") as f:
        data = json.load(f)


    x = {k:[data[k]] for k in agent.dofs.names}
    y = {k:[data[k]] for k in agent.objectives.names}
    metadata = {k:[data.get(k, None)] for k in metadata_keys}
    agent.tell(x=x, y=y, metadata=metadata)

agent._construct_models()

# print(agent.ask("qei", n=1))
# print(agent.ask("qr", n=36))