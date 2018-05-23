"""Sample class"""

class cvcCalcTiMean:
    def __init__(self, inputs, outputs, metadb_info):
        self.inputs = inputs
        self.outputs = outputs
        self.metadb = metadb_info
    
    def run(self):
        print("(cvcCalcTiMean::run) Started!")
        