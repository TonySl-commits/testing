"""
This module contains a class for prompt properties used in generating PDF reports.
The `PromptProperties` class is responsible for storing properties used to generate
a PDF report. This class uses the `CDR_Properties` class to get the properties from
the `cdr_trace.properties` file.

Attributes:
prompt (dict): A dictionary of dictionaries. The keys of the outer dictionary are the names of the prompts.
The keys of the inner dictionary are the names of the titles of the report. Each topic has a systemprompt,
user prompt and figure if it exists.

The system prompts are edited underneath their definition within the class. Whereas the user prompts are
given their values to the object defined in aoi_report_functions.py. 

This architecture allows the pdg_report_generator to tackle each topic and generate a response from the groq model.
This allows flexibility in terms of prompt engineering to get the desired output

In the case that a second approach was desired in which one request is sent to the groq model per device, then the
system prompts and user prompts should be merged in a single dictionary and read by the groq model. This will make
less requests to the model but at the cost of larger more complicated prompts

Authors: Ziad Mokdad (2024) Nour Bou Nasr (2024)
Feel free to contact if you have any questions: ziadmokdad2010@gmail.com
"""

from vcis.utils.properties import CDR_Properties

class PromptProperties:
    def __init__(self):
        self.properties = CDR_Properties()
        self.prompt = {
            "geo_spatial":{
                "system":"",
                "user":"",
                "figure": None
            },
            "hits_per_dow":{
                "system":"",
                "user":"",
                "figure": None
            },
            "hits_per_month":{
                "system":"",
                "user":"",
                "figure": None
            },
            "consistency_metrics":{
                "system":"",
                "user":"",
                "figure": None
            },
            "confidence_score": {
                "system": "",
                "user":"",
                "figure": None
            },
            "analytical_insights":{
                "system":"",
                "user":"",
                "figure": None
            },
            "location_likelihood":{
                "system":"",
                "user":"",
                "figure": None
            },
            "duration_at_aoi":{
                "system":"",
                "user":"",
                "figure": None
            },
            "suspiciousness_evaluation":{
                "system":"",
                "user":"",
                "figure": None
            },
            "add_ons_home_aoi":{
                "system":"",
                "user":"",
                "figure": None
            }
        }

        # Now populate system strings that need self references
        self.prompt['geo_spatial']['system'] = """[INST]
            <<SYS>>
            You are tasked with stating the given device ID, start date, end date, total hits, and total days. Your answer will be in a format
            of a paragraph which will be part of a report.
            <</SYS>>"""
        self.prompt['hits_per_dow']['system'] = """[INST]
            <<SYS>>
            You are tasked with stating the highest number of hits per day of the week along with the lowest. You also have to give a small analysis
            the rest of the days and their number of hits respectively. Your answer will be in a format of a paragraph which will be part of a report.
            <</SYS>>"""
        self.prompt['hits_per_dow']['figure'] = self.properties.passed_filepath_reports_png + 'number_of_hits_per_dow.jpg'
        self.prompt['hits_per_month']['system'] = """[INST]
            <<SYS>>
            You are tasked with stating the highest number of hits per month along with the lowest. You also have to give a small analysis
            the rest of the months and their number of hits respectively. Your answer will be in a format of a paragraph which will be part of a report.
            <</SYS>>"""
        self.prompt['hits_per_month']['figure'] = self.properties.passed_filepath_reports_png + 'number_of_hits_per_month.jpg'
        self.prompt['consistency_metrics']['system'] = """[INST]
            <<SYS>>
            You are tasked with analyzing the four different consistency metrics. which are:
            1. Time Period Consistency: This metric evaluates how consistently data is recorded over the specified period of time. 
                A higher score indicates that the data is consistently available throughout the specified timeframe, 
                allowing for reliable trend analysis and pattern identification.
            2. Hour of Day Consistency: This metric assesses the consistency of data recording throughout the day, 
                ensuring that data points are evenly distributed across different hours throughout the specified period of time. 
                A higher score indicates that data is consistently collected throughout the day, enabling accurate temporal analysis and insights into a person’s trajectories, 
                stay-points or areas of interest during a day.
            3. Day of Week Consistency: This metric measures the consistency of data recording across different days of the week. 
                A higher score indicates that data is consistently available across all days of the week, 
                facilitating comparative analysis between weekdays and weekends and providing valuable insights into user behavior.
            4. Hits Per Day Consistency: This metric evaluates the consistency of activity levels recorded per day, 
                providing insights into user behavior patterns over time. A higher score indicates that activity levels are consistently recorded each day, 
                offering valuable insights into user behavior patterns and enabling more accurate analysis and decision-making.
            Your answer will be in a format of a paragraph which will be part of a report.
            <</SYS>>"""
        self.prompt["confidence_score"]['system'] = f"""[INST]
            <<SYS>>
            You are tasked with analyzing the confidence score based on the consistency metrics which are: {self.prompt['consistency_metrics']['user']}. 
            Your answer will be in a format of a paragraph which will be part of a report.
            <</SYS>>"""
        self.prompt['location_likelihood']['system'] = """[INST]
            <<SYS>>
            You are tasked with analyzing a bar plot of a location likelihood of a certain device with respect to the day of week. Your role is to:
            1. Receive the information in any form possible, which is in a text format from the user. The form of the text will be a Day of the week, followed by the locations during that day and their respective likelihoods. 
            2. Tell which are the 3 most significant locations that the user is visiting during the days of the week
            3. Take into account that the 'Other' location is not a specific location the device has stayed in for a long duration, it might indicate being on the road, having a walk, or a stop somewhere briefly
            4. Give me your own analysis on the data as an AI chatbot 
            5. The generated text should not include a first person point of view when explaining
            6. Do not start the answer with based on the analysis
            7. Segment the different ideas to be displayed under 2 different subtitles, observation and analysis, without **
            8. Do not include a last line as a conclusion
            <</SYS>>"""
        self.prompt['location_likelihood']['figure'] = self.properties.passed_filepath_reports_png + 'location_likelihood_per_dow.jpg'
        self.prompt['duration_at_aoi']['system'] = """[INST]
            <<SYS>>
            You are tasked with analyzing a bar plot of a location likelihood of a certain device with respect to the day of week. Your role is to:
            1. Receive the information in any form possible, which is in a text format from the user. The form of the text will be a Day of the week, followed by the locations during that day and their respective likelihoods. 
            2. Tell which are the 3 most significant locations that the user is visiting during the days of the week
            3. Take into account that the 'Other' location is not a specific location the device has stayed in for a long duration, it might indicate being on the road, having a walk, or a stop somewhere briefly
            4. Give me your own analysis on the data as an AI chatbot 
            5. The generated text should not include a first person point of view when explaining
            6. Do not start the answer with based on the analysis
            7. Segment the different ideas to be displayed under 2 different subtitles, observation and analysis, without **
            8. Do not include a last line as a conclusion
            <</SYS>>"""
        self.prompt['duration_at_aoi']['figure'] = self.properties.passed_filepath_reports_png + 'duration_at_aoi_wrt_dow.jpg'
        self.prompt['add_ons_home_aoi']['system'] = """[INST]
            <<SYS>>
            You are tasked with describing the user's home area of interest, and other areas that might be classified as work or something else.
            Your answer will be in a format of a paragraph which will be part of a report.
            <</SYS>>"""