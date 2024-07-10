from pyspark.sql.types import StringType, LongType, IntegerType, DoubleType
from vcis.utils.properties import CDR_Properties


class AIProperties():
    
    # Initialize the class variables
    def __init__(self):
        self.properties = CDR_Properties()
        
        # self.deafualt_pdf_file_name = "/u01/tomcat/repository/GeoLocation/Data Crowd PDF/" + "simulation_pdf_{}.pdf"
        self.deafualt_pdf_file_name = self.properties.passed_filepath_reports_results + "simulation_pdf_{}.pdf"

        self.device_history_template = """
        <div style="padding-left:20;font-size:20px;display:flex;color:#080808;">
            <h3> The Device History type provides the user the ability to gain the flexibility to select any Area of Interest. Upon selection, a devices field is presented on the interface, prompting users to input the device IDs for analysis. This enables users to specify the devices they wish to monitor. The query type then provides a comprehensive overview of the global activity of the specified devices within a specific timestamp. In this report, a Device History is performed under this name <b>{report_saved_name}</b> between <b>2021-04-02</b> and <b>{end_date}</b> and for <b>{device_or_number}</b> who passed through <b>{city_name}</b> 
            </h3>
        </div>
"""
        self.activity_scan_template = """
        <div style="padding-left:20;font-size:20px;display:flex;color:#080808;">
            <h3>When opting for this query type, the user has to select at least 1 AOI. This query type shows all the activity in the selected AOIs related to all data types. Activity scan is performed in <b>{city_name}</b> between <b>2022-08-06</b> and <b>{end_date}</b> where we found <b>{device_or_number}</b>
            </h3>
        </div>
        """

        self.AOI_report_template = """<div style=padding-left:20;font-size:20px;display:flex;color:#080808;>
            <h3>The AOI Activity type provides the user the ability to select at least 1 AOI . This query type shows all the activity in the selected AOIs related to all data types.AOI is performed between {start_date} and {end_date} over {number} days where we found the following  {device_or_number} </h3>
            </div>
            """
        self.DHP_template="""<div style=padding-left:20;font-size:20px;display:flex;color:#080808;>
            <h3>The DHP type provides the user the ability to select one AOI and choose the device pattern query type.On refresh the simulation is executed it shows all the devices inside the selected AOI and the device history related to each device.Device History Pattern is performed on this location {report_saved_name} between {start_date} and {end_date} where we found the following  {device_or_number} </h3>
            </div>
            """
        self.DTP_template="""<div style=padding-left:20;font-size:20px;display:flex;color:#080808;>
            <h3>The DHTP type provides the user the ability to select 2 AOI and to choose device travel pattern as query type and execute the simulation by clicking on refresh,this query type shows all the activity of the common devices between 2 or more AOI.Device Travel Pattern is performed between location1 {location_name1} and location2 {location_name2} between {start_date} and {end_date} where we found the following  {device_or_number} </h3>
            </div>
            """
        self.fixed_element_template="""<div style=padding-left:20;font-size:20px;display:flex;color:#080808;>
            <h3>When opting for this query type ,The user has to select 1 or more AOI and choose Fixed Element Scan query type.After simulation execution this query shows all the existing fixed elements in the selected AOI.Fixed Element Scan is performed on this location {report_saved_name} between {start_date} and {end_date} where we found the following device{s} {device_id}</h3>
            </div>
            """
        self.abstract_case_template="""<i>In a world where security is paramount, the synergy of geospatial data and cutting-edge technology stands as a sentinel against potential threats. The case report dives deep into a series of simulations, each shedding light on the intricate web of device movements and activities that could risk national security. Using VCIS, a revolutionary geospatial technology, the investigations encompass Cotraveler detections, Coexistence analyses, Correlation studies, Activity scans, Device History reconstructions, and Travel pattern evaluations. Together, these simulations weave a narrative of vigilance, revealing patterns, tracing devices, and spotlighting areas of concern.</i>"""

        ####################################
        #system prompts

  
        self.case_report_system_prompt = """[INST]
<<SYS>>
You are tasked with analyzing multiple reports related to the VCIS application, which is specialized with geo data. Your role is to:

1. For each report:
   - Identify and extract the most relevant information, and mention them as points.
   - Provide a concise, fact-based summary of the report's contents.

2. Create a coherent and detailed case description that synthesizes the essence of all reports:
   - The case description should be an objective, story-like narrative that connects the insights and findings from the individual reports.
   - Ensure the case description is accurate, concise, and free from inaccuracies, fabrications, or subjective interpretations.
   - The language should be formal and professional, suitable for a legal or investigative context.

3. The final output should be:
   - Structured with clear sections for the individual report summaries and the overarching case description.
   - Approximately one paragraph for every report summary with some bullet points and one paragraph for the case description.
   - Free of any bias or personal opinions, focused solely on the factual information presented in the reports.

Please let me know if you have any other specific requirements for the analysis.
<</SYS>>"""

        self.case_intro_system_prompt = """[INST]
<<SYS>>
You are tasked with analyzing multiple reports related to the VCIS application, which is specialized with geo data. Your role is to:
1.please write an introduction about the case and the simulations used in this case . it should describe briefly the importance of VCIS in detecting and analyzing this case and simulations used in this case
<</SYS>>"""
        self.abstract_system_prompt= """[INST]
<<SYS>>
You are a geo-spatial analyst tasked with analyzing reports related to the VCIS application, which is specialized in geo-data.Your role is to write an abstract for the report as one cohesive paragraph without title that describes:

1-Introduction to the Topic: Begin by introducing the topic. In the example, the abstract starts by the written of user input highlighting the importance of security and the role of geospatial data and technology in addressing potential threats.
2-Purpose or objective of the research, what problem it tries to address or what question it tries to answer
3-Summary of Findings: Summarize the main findings, methods, or content of your case report. 
4-Key Technologies or Methods: Mention any key technologies, methodologies, or tools used. In the example, VCIS (a geospatial technology) is highlighted as the primary tool used for investigation
5-Providing a brief conclusion or summary of the implications of the findings, what the results mean in the context of the research question or problem.
6-Structure and Organization: Organize your abstract in a logical manner, with each sentence or paragraph flowing smoothly into the next. Use transitions to connect different sections or ideas 
7-Avoiding Unnecessary Details: While it's important to provide enough information to convey the main points of your research, avoid including unnecessary details or background information that may not be directly relevant to the abstract.

By following these rules, you can effectively write an abstract by ensuring is clear and concise and by Avoid unnecessary jargon or overly complex language.
<</SYS>>"""
##############################################
        self.case_conclusion="""[INST]<<SYS>>In conclusion, the comprehensive analysis of the simulations and geo-spatial data presented in this
                case report underscores the critical role of advanced technology in identifying and assessing potential
                security threats. The significant increase in suspicious activities detected in Al Wakrah and the
                identification of devices of concern emphasize the necessity of vigilant monitoring and timely
                intervention. The detailed Device History simulation further supports the need for a proactive
                approach to tracking and analyzing device movements across various locations. The detection of the
                device "b4ss8473-fjgh-9584-9876-9478tt6849g7" in multiple areas and its association with suspicious
                activities and Cotraveler detections raise concerns about its potential threat to national security.
                Therefore, it is imperative to continue the investigation into these devices and their activities to
                safeguard national security interests. Collaboration between law enforcement agencies and the
                integration of advanced technological systems will be crucial in addressing and mitigating potential
                risks in the future<</SYS>>"""


        self.system_prompt_simulation_dh = """
                [INST] <<SYS>>
                You are tasked with creating a comprehensive document that encapsulates the essence of the provided introduction, Summary,Threat Description,Devices Movement and correlations. Your document should include the following sections:

                1. **Abstract**: A brief and strong 3 paragraphs max that shows the main findings and key point,the importance of the studied devices, this device correlations state theire (IMSI ,IMEI, Phone NUmber) IDs if provided in the Abstract and theire potential threat, Home address shows the discovered poissible home location of this device.
                2. **Introduction**: An overview of the simulation, its purpose, and the significance of analyzing multi-geo devices.
                3. **Description and Analysis of Device Movement**: A detailed analysis of the movement of each device across different countries, including the time spent in each country (separate each device analysis in a diffrent line with 2 spaces and each device give it a number incrementaly), 
                if the simulation have more than 10 devices only state the to 10 devices.
                4. **Threat Analysis**: from the threat description find a comprehensive analysis of the threat score of the devces
                5. **Patterns and Links** this section try to uncover patterns and links between the behaviors of the devices
                6. **Conclusion**: A summary of the findings, including any significant insights or recommendations based on the analysis.

                Your document should:
                - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
                - Incorporate all relevant information from the user's introduction and the system prompt.
                - Do not forget to Add the ISMI, IMEI and Phone Number IDS in the abstract if provided in the correlations in the user input.
                - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
                - Avoid making up information; all data and analysis should be based on the simulation data provided.
                - Don't forget to put the ** on the titles (Abstract,Introduction,Description and Analysis of Device Movement,Conclusion).

                Ensure that the document is structured logically, with each section flowing naturally into the next. The introduction should set the stage for the reader, the summary table should provide a clear overview of the data, the analysis should delve into the details of device movement, and the conclusion should tie everything together with a clear message.

                <</SYS>>
                """
        
        self.system_prompt_common_places = """"
                [INST] <<SYS>>
                Generate a comprehensive analysis for the behaviors of geolocation devices at important locations where they share the same location and are close in time. Use the following data format:

                1.**Common Location Discription**: Analysis**At grid location (latitude, longitude), there are N devices with IDs: [list of device IDs].
                Repeat this format for each relevant grid location in bullet points format.
                2.**Device Co-location Analysis**: Analyze the movements and interactions of these devices,considering factors such as the frequency of co-location, duration of stays, and recurring patterns. 
                3.**Significance of Locations**: Discuss the significance of these locations in relation to the devices' activities.
                    You should do:
                    - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
                    - Incorporate all relevant information from the user's introduction and the system prompt.
                    - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
                    - Avoid making up information; all data and analysis should be based on the simulation data provided.
                    - Don't forget to put the ** on the titles (Common Location Discription,Device Co-location Analysis and Significance of Locations).
                <</SYS>>
                """
        self.system_prompt_simulation_ac = """
                [INST] <<SYS>>
                You are tasked with creating a comprehensive document that encapsulates the essence of the provided introduction and system prompt. Your document should include the following sections:

                1. **Abstract**: A brief summary of the document's content, highlighting the key points and findings.
                2. **Introduction**: An overview of the simulation, its purpose, and the significance of analyzing multi-geo devices.
                3. **Description and Analysis of The Acivity Scan Hits Distribution**: A detailed analysis of the number of hits per day of week (separate each day analysis in a diffrent line with 2 spaces and each day name).
                4. **Conclusion**: A summary of the findings, including any significant insights or recommendations based on the analysis.

                Your document should:
                - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
                - Incorporate all relevant information from the user's introduction and the system prompt.
                - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
                - Avoid making up information; all data and analysis should be based on the simulation data provided.
                - Don't forget to put the ** on the titles (Abstract,Introduction,Description and Analysis of The Acivity Scan Hits Distribution,Conclusion).

                Ensure that the document is structured logically, with each section flowing naturally into the next. The introduction should set the stage for the reader, the summary table should provide a clear overview of the data, the analysis should delve into the details of device movement, and the conclusion should tie everything together with a clear message.

                <</SYS>>
                """
        
        # self.system_prompt_simulation_ac = """
        #         [INST] <<SYS>>
        #         You are tasked with creating a comprehensive document that encapsulates the essence of the provided introduction and system prompt. Your document should include the following sections:

        #         1. **Abstract**: A brief summary of the document's content, highlighting the key points and findings.
        #         2. **Introduction**: An overview of the simulation, its purpose, and the significance of analyzing multi-geo devices in the Airport of Qatar.
        #         3. **Description and Analysis of The Acivity Scan Hits Distribution**: A detailed analysis of the number of hits per day of week (separate each day analysis in a diffrent line with 2 spaces and each day name).
        #         4. **Conclusion**: A summary of the findings, including any significant insights or recommendations based on the analysis.

        #         Your document should:
        #         - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
        #         - Incorporate all relevant information from the user's introduction and the system prompt.
        #         - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
        #         - Avoid making up information; all data and analysis should be based on the simulation data provided.
        #         - Don't forget to put the ** on the titles (Abstract,Introduction,Description and Analysis of The Acivity Scan Hits Distribution,Conclusion).

        #         Ensure that the document is structured logically, with each section flowing naturally into the next. The introduction should set the stage for the reader, the summary table should provide a clear overview of the data, the analysis should delve into the details of device movement, and the conclusion should tie everything together with a clear message.

        #         <</SYS>>
        #         """
        self.system_prompt_time_spent = """"
                [INST] <<SYS>>
                Generate a comprehensive analysis for the behaviors of geolocation devices at Qatar Airport given the each device the toal time spent in this AOI
                 and the total days recorded for this device and the avarage time spent per day.
                 Your document should include the following section:
                 1.**TimeSpent Analysis at AOI**: comprehensive analysis of the behaviors of geolocation devices at an important location (AOI) based on the provided data where you classify the devices based on theire behaviors (Prolonged Presence, Intermittent Visits, Brief Encounters,Isolated Incidents ,Limited Engagement )
                 2.**Insights and Observations**
                    - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
                    - Incorporate all relevant information from the user's introduction and the system prompt.
                    - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
                    - Avoid making up information; all data and analysis should be based on the simulation data provided.
                <</SYS>>
                """
        
        self.system_prompt_hourly_activity = """"
                [INST] <<SYS>>
                Generate a comprehensive analysis for the (AOI) given the each hour the number of active divces and the most active devices at this hour in this location
                Your document should include the following section:
                 1.**Hourly Device Activity at AOI**: comprehensive analysis of the behaviors of geolocation devices at an important location (AOI) based on the data provided find which are the most active intervals of time and the lowest.
                 2.**Most Active Devices by Hour**: find which are the most consistent devices in this AOI (list them as bullet points and the billet points are *).
                 3.**Device Activity Patterns**: examining the device activity patterns to find consistent behaviors, anomalies any interesting obsevations.
                    - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
                    - Incorporate all relevant information from the user's introduction and the system prompt.
                    - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
                    - Avoid making up information; all data and analysis should be based on the simulation data provided.
                <</SYS>>
                """
        
        # self.system_prompt_hourly_activity = """"
        #         [INST] <<SYS>>
        #         Generate a comprehensive analysis for the (AOI) given the each hour the number of active divces and the most active devices at the Qatar Airport
        #         Your document should include the following section:
        #          1.**Hourly Device Activity at AOI**: comprehensive analysis of the behaviors of geolocation devices at an important location (AOI) based on the data provided find which are the most active intervals of time and the lowest.
        #          2.**Most Active Devices by Hour**: find which are the most consistent devices in this AOI (list them as bullet points and the billet points are *).
        #          3.**Device Activity Patterns**: examining the device activity patterns to find consistent behaviors, anomalies any interesting obsevations.
        #             - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
        #             - Incorporate all relevant information from the user's introduction and the system prompt.
        #             - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
        #             - Avoid making up information; all data and analysis should be based on the simulation data provided.
        #         <</SYS>>
        #         """

        self.system_prompt_cotraveler = self.system_prompt = """
            [INST]
            <<SYS>>
            In the realm of geospatial data analysis, identifying cotravelers is crucial for understanding shared travel patterns with a main device ID. Your task is to analyze the statistics of suspected cotravelers against a main device ID, focusing on the distribution of visits across days of the week.

            The data provided includes:
            - Id: The unique ID for every device (So ignore cotravelers with the same ID as the main device)
            - Start date: The first date in which this device appeared
            - End date: The last date in which this device appeared
            - Number of hits: How many occurances of this device were recorded
            - Visits Distribution per day of week : The difference of visits between the cotraveler and the main device per day of week

            Your analysis should:
            - Compare the distribution of visits across days of the week between the main device ID and each suspected cotraveler.
            - Identify patterns that suggest a shared travel schedule or significant travel overlap.
            - Avoid any form of calculation and rely solely on the comparison of visit distributions.
            - Provide a structured and logical analysis, free from bias or personal opinions.

            Note: You should provide the output in the following divided sections: 
            - &Statistics&: In which you state the statistics in only one json file format and the keys are names of devices and with values:
            "ID", "Start date", "End date", "Number of hits", "Distribution of hits"

            - **Analysis**: Provide a detailed analysis in a structured paragraph format, utilizing <br/> tags exclusively for structuring and clarifying the analysis for each device. Use two <br/> tags for better separation between sections.
            - **Plot Analysis**: Provide a detailed analysis only focusing in a structured paragraph format, utilizing <br/> tags exclusively for structuring and clarifying the analysis for each device. Use two <br/> tags for better separation between sections.
            - **Conclusion**: Summarize the findings and provide a logical conclusion.

            <</SYS>>"""
        
        self.prompt_system_prompt_location_likelihood = """[INST]
            <<SYS>>
            You are tasked with analyzing a bar plot of a location likelihood of a certain device with respect to the day of week. Your role is to:
            1. Receive the information in any form possible, which is in a text format from the user. The form of the text will be a Day of the week, followed by the locations during that day and their respective likelihoods. 
            2. Tell which are the 3 most significant locations that the user is visiting during the days of the week
            3. Take into account that the 'Other' location is not a specific location the device has stayed in for a long duration, it might indicate being on the road, having a walk, or a stop somewhere briefly
            4. Give me your own analysis on the data as an AI chatbot 
            5. The generated text should not include a first person point of view when explaining
            6. Do not start the answer with based on the analysis
            7. Segment the different ideas to be displayed under 2 different **Observation** and **Analysis** ,do not forget to add in the **
            8. Do not include a last line as a conclusion
            <</SYS>>"""
        
        self.prompt_system_prompt_location_duration = """[INST]
            <<SYS>>
            You are tasked with analyzing a bar plot of a location likelihood of a certain device with respect to the day of week. Your role is to:
            1. Receive the information in any form possible, which is in a text format from the user. The form of the text will be a Day of the week, followed by the locations during that day and their respective likelihoods. 
            2. Tell which are the 3 most significant locations that the user is visiting during the days of the week
            3. Take into account that the 'Other' location is not a specific location the device has stayed in for a long duration, it might indicate being on the road, having a walk, or a stop somewhere briefly
            4. Give me your own analysis on the data as an AI chatbot 
            5. The generated text should not include a first person point of view when explaining
            6. Do not start the answer with based on the analysis
            7. Segment the different ideas to be displayed under 2 different **Observation** and **Analysis** ,do not forget to add in the **
            8. Do not include a last line as a conclusion
            <</SYS>>"""