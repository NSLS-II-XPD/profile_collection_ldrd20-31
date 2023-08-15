"""
Run:

$ BS_ENV=2023-2.1-py310-tiled-local BS_PYTHONPATH="$HOME/src/BayesianOptimization/src" bsui

"""
import os

import configparser
from qd_exp import QDexp


problem = QDexp(problem_cfg=os.path.expanduser("~/src/BayesianOptimization/config/problem/qd.cfg"))
config = configparser.ConfigParser()
config.read(main_cfg)
main_cfg = "/home/xf28id2/src/BayesianOptimization/config/main.cfg"
config.read(main_cfg)
problem_cfg  = os.path.join(pkg_dir, config['Problem']['problem_cfg'])
problem_name = config['Problem']['problem_name']
algo_cfg     = os.path.join(pkg_dir, config['Algorithm']['algo_cfg'])
bo_package   = config['Algorithm']['bo_package']

pkg_dir = "/home/xf28id2/src/BayesianOptimization"
problem_cfg  = os.path.join(pkg_dir, config['Problem']['problem_cfg'])
problem_name = config['Problem']['problem_name']
algo_cfg     = os.path.join(pkg_dir, config['Algorithm']['algo_cfg'])
bo_package   = config['Algorithm']['bo_package']
print("Main Config:    ",   main_cfg)
print("Algorithm Config: ", algo_cfg)
print("Problem Config: ",   problem_cfg)
print("BO Package:     ",   bo_package)
problem.insert_output(1, 2, 0.5)

from bo_skopt import SkoptBO
bo = SkoptBO(algo_cfg)
bo.runBO(problem.objectiveFunction, problem._bounds)
