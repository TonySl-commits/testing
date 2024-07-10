from vcis.ai.tools.vcis_tools import vcisTools

vcis_tools = vcisTools()

response = vcis_tools.get_qa_over_simulation(simulation_name="Resto Area DH",question="give me the location of the last hit for each device id")
print(response['answer'])