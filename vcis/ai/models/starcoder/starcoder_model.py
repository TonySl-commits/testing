from vcis.utils.properties import CDR_Properties
from vcis.utils.utils import CDR_Utils
from transformers.tools import HfAgent
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, LocalAgent
class StarCoderAgent():
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.utils = CDR_Utils()
        self.properties = CDR_Properties()
    def get_gpus_max_memory(self,max_memory):
        max_memory = {i: max_memory for i in range(torch.cuda.device_count())}
        return max_memory

    def get_agent(self,tools,local=True):
        if local:
            available_memory = torch.cuda.get_device_properties(0).total_memory
            print("Available Memory",available_memory)
            checkpoint = "bigcode/starcoder2-7b"
            model = AutoModelForCausalLM.from_pretrained(checkpoint, 
                                                         device_map="auto", 
                                                        trust_remote_code=True,
                                                        load_in_8bit=True,
                                                        use_cache=True,
                                                        max_memory=self.get_gpus_max_memory("10GB"))
            tokenizer = AutoTokenizer.from_pretrained(checkpoint)
            agent = LocalAgent(model, tokenizer,additional_tools=tools)
        else:
            agent = HfAgent("https://api-inference.huggingface.co/models/bigcode/starcoder2-15b", additional_tools=tools)

        return agent
        

    