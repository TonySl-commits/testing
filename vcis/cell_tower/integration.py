from vcis.utils.utils import CDR_Properties , CDR_Utils
from vcis.cell_tower.preproccess import CDR_Preprocess
import pandas as pd


class CDR_Integration():

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
        self.preprocess = CDR_Preprocess()

    def merge_cdr(self, twog, threeg, LTE):
        df = pd.concat([twog,threeg,LTE])
        return df