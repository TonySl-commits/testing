import uvicorn
from fastapi import FastAPI, Body
import time
import json
import pandas as pd
import warnings
from transformers.tools import HfAgent
from transformers import Tool
import huggingface_hub 
import pandas as pd

warnings.filterwarnings('ignore')

from vcis.utils.utils import CDR_Utils
from vcis.utils.properties import CDR_Properties
from vcis.databases.oracle.oracle_tools import OracleTools
from vcis.reporting.report_main.report_generator import ReportGenerator
from vcis.databases.cassandra.cassandra_tools import CassandraTools
from vcis.correlation.cdr_correlation_functions import CDRCorrelationFunctions
from vcis.ai.tools.starcoder_tools import AgentTools
from vcis.ai.models.starcoder.starcoder_model import StarCoderAgent
##########################################################################################################
global properties
properties = CDR_Properties()

app = FastAPI()

@app.post("/chat")
# 
async def read_root(string_entity: str = Body(...)):
    # huggingface_hub.login()
    data = json.loads(string_entity)
    prompt=data['prompt']
    print(prompt)
    utils = CDR_Utils(verbose=True)
    agent_tools = AgentTools()
    agent = StarCoderAgent()
    # huggingface_hub.login()
    tools = []

    tools.append(agent_tools.get_device_history())

    tools.append(agent_tools.get_activity_scan())

    tools.append(agent_tools.get_cotraveler())
    
    # tools.append( agent_tools.get_simulation())

    tools.append(agent_tools.get_polygon_scan_saved_shape())
    
    tools.append(agent_tools.get_device_history_travel_pattern())

    tools.append(agent_tools.get_generate_report_from_simulation())
    # tools.append(agent_tools.get_qa_over_simualtion())

    model = agent.get_agent(tools = tools,local=False)
    answer = model.run(prompt)

    return answer

if __name__ == "__main__":
    uvicorn.run(app, host=properties.api_host, port=properties.api_port_agent , reload=False) 