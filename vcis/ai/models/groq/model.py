from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils

from langchain_groq.chat_models import ChatGroq
from pandasai.llm.local_llm import LocalLLM
from langchain_core.output_parsers import StrOutputParser

class GroqAIModel():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()

    def get_model(self,model_name:str="llama3-70b-8192", local=True):
        if local:
            llm = LocalLLM(api_base="http://localhost:11434/v1/", model="codellama")


        else:
            llm = ChatGroq(
                model_name=model_name, 
                api_key = "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")
        return llm
    

    def get_chain(self,llm, system_prompt:str=None,user_prompt:str=None):
        full_prompt = f"{system_prompt}{user_prompt}[/INST]"
        chain = llm | StrOutputParser()
        ai_respone = chain.invoke(full_prompt)
        return ai_respone
    
    