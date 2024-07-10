from langchain_community.llms import Ollama
from langchain.prompts import ChatPromptTemplate, MessagesPlaceholder, HumanMessagePromptTemplate

# Define the system prompt
system_prompt = """
[INST] <<SYS>>
You are tasked with analyzing multiple reports related to the VCIS application, which is specialized with geo data. Your role is to:

- Identify and extract the most relevant information from each report.
- Create a coherent and detailed case description that encapsulates the essence of all reports.
- Ensure the case description is accurate, concise, and free from inaccuracies or fabrications.
- Your language should be formal and professional, suitable for a legal or investigative context.

<</SYS>>
"""
# Define the user input

user_input = """
report discription 1 : In this report, an Acvtity scan is performed under this Area name Resto Area DH between 2022-03-02 and 2022-09-05 and  6 devices where returend,

report discription 2 : In this report, a Device History is performed under this simulation name jp between 2022-03-02 and 2022-04-02 and for the device b4ss8473-fjgh-9584-9876-9478tt6849g7,

report discrition 3 : In this report, a Cotraveler detection is performed  between 2022-03-02 and 2022-04-02 for the device b4ss8473-fjgh-9584-9876-9478tt6849g7
"""
# Construct the full prompt including the system prompt and user input
full_prompt = f"{system_prompt}{user_input} [/INST]"

# Initialize the Ollama model
llm = Ollama(model="llama2:latest")
# llm.temperature = 0
# Invoke the model with the full prompt
response = llm.invoke(full_prompt)

print(response)










