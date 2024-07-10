from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils

from langchain_groq.chat_models import ChatGroq
from pandasai.llm.local_llm import LocalLLM
import pandas as pd
from pandasai import SmartDataframe

class PandasAIModel():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

    def get_model(self,local=True):
        if local:
            llm = LocalLLM(api_base="http://localhost:11434/v1/", model="codellama")


        else:
            llm = ChatGroq(
                model_name="llama3-70b-8192", 
                api_key = "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")
        return llm
    def get_dataframe(self,data,llm):
        df = SmartDataframe(data, config={"llm": llm,
                                        "verbose": True,
                                        "save_charts": True,
                                        "save_charts_path": self.properties.passed_filepath_reports_png})
        return df 

    