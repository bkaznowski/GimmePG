import glob
from pathlib import Path

import yaml

class Resources():

    def __init__(self, path=None, yamls=None):
        if path:
            path = path[:-1] if path[-1] == "\\" else path
            files = glob.glob(f"{path}/*.yaml")
            self.resources = {}
            for f in files:
                with open(f, "r") as stream:
                    self.resources[Path(f).stem] = yaml.safe_load(stream)
        if yamls:
            for k,v in yamls.items():
                self.resources[k] = yaml.safe_load(v)


    def get_constants(self, name):
        return self.resources[name].get("constants", {})

    def get_variables(self, name):
        return self.resources[name].get("variables", {})

    def get_operations(self, name):
        return self.resources[name].get("operations", [])
