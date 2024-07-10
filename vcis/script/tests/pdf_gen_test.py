from vcis.utils.properties import CDR_Properties
from vcis.ai.tools.vcis_tools import vcisTools
from vcis.ai.models.pandasai.pandasai_model import PandasAIModel
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors, units
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from hashlib import sha1
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, NextPageTemplate, PageBreak, Image
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
import re
vcis_tools = vcisTools()
properties = CDR_Properties()
pandasai = PandasAIModel()
respone ,summary_table= vcis_tools.get_simulation_pdf(simulation_id=176796)


# print(respone['simulation_summary'])
# print(respone['simulation_intro'])
# print(respone['devices_movments'])
# print(respone['common_grids'])

model = pandasai.get_model(local=False)

system = "You are a helpful assistant."


user_prompt = f"""

    Introduction : {respone['simulation_intro']}

    Summary :{respone['simulation_summary']}

    Devices Movement: {respone['devices_movments']}

    """

system_prompt = """
    [INST] <<SYS>>
    You are tasked with creating a comprehensive document that encapsulates the essence of the provided introduction and system prompt. Your document should include the following sections:

    1. **Abstract**: A brief summary of the document's content, highlighting the key points and findings.
    2. **Introduction**: An overview of the simulation, its purpose, and the significance of analyzing multi-geo devices.
    3. **Description and Analysis of Device Movement**: A detailed analysis of the movement of each device across different countries, including the time spent in each country (separate each device analysis in a diffrent line with 2 spaces and each device give it a number incrementaly).
    4. **Patterns and Links** this section try to uncover patterns and links between the behaviors of the devices
    5. **Conclusion**: A summary of the findings, including any significant insights or recommendations based on the analysis.

    Your document should:
    - Engage the reader from the first sentence, capturing interest and maintaining it throughout.
    - Incorporate all relevant information from the user's introduction and the system prompt.
    - Maintain a formal and professional tone, suitable for a narrative that could be shared in a legal or investigative context.
    - Avoid making up information; all data and analysis should be based on the simulation data provided.
    - Don't forget to put the ** on the titles (Abstract,Introduction,Description and Analysis of Device Movement,Conclusion).

    Ensure that the document is structured logically, with each section flowing naturally into the next. The introduction should set the stage for the reader, the summary table should provide a clear overview of the data, the analysis should delve into the details of device movement, and the conclusion should tie everything together with a clear message.

    <</SYS>>
    """

full_prompt = f"{system_prompt}{user_prompt}[/INST]"

# prompt_template.format(introduction=respone['simulation_intro'], summary=respone['simulation_summary'] , devices_movement = respone['devices_movments'])
# print(prompt_template)
# chain = prompt | model

chain = model | StrOutputParser()
ai_respone = chain.invoke(full_prompt)
# print(ai_respone)
abstract_start_marker = '**Abstract**'
abstract_end_marker = '**Introduction**'

# Find the start and end indices of the answer
start_index = ai_respone.find(abstract_start_marker) + len(abstract_start_marker)
end_index = ai_respone.find(abstract_end_marker) 

# Extract the answer
abstract = ai_respone[start_index:end_index]
# print(abstract)

introduction_start_marker = "**Introduction**"

introduction_end_marker = "**Description and Analysis of Device Movement**"
start_index = ai_respone.find(introduction_start_marker) + len(introduction_start_marker)
end_index = ai_respone.find(introduction_end_marker) 

introduction = ai_respone[start_index:end_index]

# print(introduction)



movement_analysis_start_marker = "**Description and Analysis of Device Movement**"

movement_analysis_end_marker = "**Patterns and Links**"

start_index = ai_respone.find(movement_analysis_start_marker) + len(movement_analysis_start_marker)
end_index = ai_respone.find(movement_analysis_end_marker) 

movement_analysis = ai_respone[start_index:end_index]

# print(movement_analysis)

patterns_links_start_marker = "**Patterns and Links**"

patterns_links_end_marker = "**Conclusion**"

start_index = ai_respone.find(patterns_links_start_marker) + len(patterns_links_start_marker)
end_index = ai_respone.find(patterns_links_end_marker) 

patterns_links = ai_respone[start_index:end_index]

conclusion_start_marker = "**Conclusion**"


start_index = ai_respone.find(conclusion_start_marker) + len(conclusion_start_marker)
end_index = -1

conclusion = ai_respone[start_index:end_index]

common_places_system_prompt = """"
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

user_prompt = respone['common_grids']
full_prompt = f"{common_places_system_prompt}{user_prompt}[/INST]"
ai_respone = chain.invoke(full_prompt)
# print(ai_respone)
common_location_marker_start_marker = "**Common Location Descriptions**"

common_location_marker_end_marker = "**Device Co-location Analysis**"
start_index = ai_respone.find(common_location_marker_start_marker) + len(common_location_marker_start_marker)
end_index = ai_respone.find(common_location_marker_end_marker) 

common_location = ai_respone[start_index:end_index]


device_colocation_start_marker = "**Device Co-location Analysis**"

device_colocation_end_marker = "**Significance of Locations**"
start_index = ai_respone.find(device_colocation_start_marker) + len(device_colocation_start_marker)
end_index = ai_respone.find(device_colocation_end_marker) 

device_colocation = ai_respone[start_index:end_index]

significane_location_start_marker = "**Significance of Locations**"


start_index = ai_respone.find(significane_location_start_marker) + len(significane_location_start_marker)
end_index = -1

significane_location = ai_respone[start_index:end_index]

style = TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), '#03045E'), # Dark blue header background
    ('TEXTCOLOR', (0, 0), (-1, 0), '#FFFFFF'), # White header text
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'), # Left alignment for all cells
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), # Bold font for headers
    ('FONTSIZE', (0, 0), (-1, 0), 14),
    ('TOPPADDING', (0, 0), (-0.5, 0), 12), # Top padding for headers
    ('BOTTOMPADDING', (0, 0), (-0.5, 0), 12),
    ('BACKGROUND', (0, 1), (-1, -1), '#FFFFFF'), # White background for body
    ('TEXTCOLOR', (0, 1), (-1, -1), '#000000'), # Black text for body
    ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'), # Different font for body
    ('GRID', (0,0), (-1,-1), 0.7, '#000000'), # Black grid lines
])

summary_table.setStyle(style)

# # Logo
logo_path = properties.report_valoores_png_path

image="plot.png"
intro_image = properties.passed_filepath_reports_png +'intro_image.jpg'
simulation_image_1 = properties.passed_filepath_reports_png +'simulation_image_1.png'
title = "Simulation Report"
# Define your document template
class SimpleDocTemplate(BaseDocTemplate):
    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
        self.pagesize = letter

    #called after every flowable (element) is added to the document
    def afterFlowable(self, flowable):
        "Registers TOC entries."
         # Check if the flowable is a Paragraph
        if flowable.__class__.__name__ == 'Paragraph':
            # Extract plain text and style name of the paragraph
            text = flowable.getPlainText()
            style = flowable.style.name
            
             # Assign a level to the TOC entry based on the style name
            if style == 'Heading1':
                level = 0
            elif style == 'Heading2':
                level = 1
            else:
                return
            
            # Create a tuple containing TOC level, text, and page number
            E = [level, text, self.page]
            bn = getattr(flowable, '_bookmarkName', None)
            if bn is not None: E.append(bn)
            self.notify('TOCEntry', tuple(E))
            
    def handle_pageBegin(self):
        '''override base method to add a change of page template after the first page.
        '''
        self._handle_pageBegin()
        if self.page == 1:
            self._handle_nextPageTemplate('FirstPage')
        else:
            self._handle_nextPageTemplate('Later')
            
class ReportPDFGenerator:
    
    def __init__(self, filename):
        self.filename = filename
        self.inch = units.inch
        self.styles = getSampleStyleSheet()
        self.setup_styles()
   
    def setup_styles(self):

        body_style = self.styles['BodyText']
        body_style.fontSize = 11
        body_style.textColor= colors.HexColor('#002147')
        body_style.leading = 30
        
        bullet_style = self.styles['BodyText']
        bullet_style.fontSize = 11
        bullet_style.leading = 25
        
        heading1_style = self.styles['Heading1']
        heading1_style.fontSize = 18
        heading1_style.textColor =  colors.HexColor('#002147')
        
        heading2_style = self.styles['Heading2']
        heading2_style.fontSize = 14
        heading2_style.textColor =  colors.HexColor('#002147')
        
       
    def add_page_number_and_footer(self, canvas, doc):
        canvas.saveState()
        canvas.setFont('Times-Roman', 10)
        page_number_text = "%d" % (doc.page)
        canvas.drawCentredString(8 * self.inch, 0.5 * self.inch, page_number_text)
        
        canvas.setFont('Times-Bold', 9) 
        canvas.setFillColor(colors.HexColor("#787878"))
        footer_text = "Copyright © 2024 - VALOORES. All rights reserved. Confidential and Proprietary."
        canvas.drawString(2 * self.inch, 0.3 * self.inch, footer_text)
        canvas.restoreState()
    
    def add_bullet_points(self, content):
        bullet_points = content.split('\n')
        bullet_content = []
        
        for point in bullet_points:
            if point.strip():
                bullet_content.append(Paragraph(f"• {point.strip()}", self.styles['BodyText']))
        
        return bullet_content
    
    def create_toc(self):
        toc = TableOfContents()
        toc_style = ParagraphStyle(
            name='TOC',
            fontSize=11,
            leading=20,
            bulletFontName='Times-Roman',
            bulletFontSize=10,
            textColor=colors.HexColor('#002147'),
        )
        toc.levelStyles = [toc_style]
        return toc
    
    
    def generate_pdf(self,movement_analysis):
        doc = SimpleDocTemplate(self.filename, pagesize=letter)
        frameT = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')
        
        first_page_template = PageTemplate(id='FirstPage', frames=frameT, onPage=self.add_page_number_and_footer)
        later_page_template = PageTemplate(id='Later', frames=frameT, onPage=self.add_page_number_and_footer)
        
        doc.addPageTemplates([first_page_template, later_page_template, PageTemplate(id='ContentPage', frames=frameT)])
        
        toc = self.create_toc()
        
        pages_content = [
            Image(logo_path, width=1.7*self.inch, height=1*self.inch, hAlign="LEFT"),
            Paragraph(title, ParagraphStyle( "TitleStyle",fontSize=50,textColor=colors.HexColor('#002147'),alignment=TA_LEFT, spaceAfter=50,spaceBefore=-0.05 )),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{abstract}", ParagraphStyle(name='body', fontSize=10, textColor=colors.HexColor('#002147'), fontName='Times-Italic', leading=15)),
            Spacer(1, 0.5*self.inch), 
            Image(intro_image, width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),
            
            NextPageTemplate('ContentPage'),
            PageBreak(),
            Paragraph('<b>Table of Contents</b>', ParagraphStyle(name='centered', fontSize=20, textColor=colors.HexColor('#002147'), leading=16, alignment=1, spaceAfter=20)),
            Spacer(1, 0.5*self.inch), 
            toc,
            PageBreak(),
            
            Image(simulation_image_1, width=9*self.inch, height=4*self.inch, hAlign="CENTER"),
            Spacer(1, 0.05*self.inch),
            Paragraph("Introduction", self.styles["Heading1"]),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{introduction}", self.styles["BodyText"]),
            Spacer(1, 0.05*self.inch),
            summary_table,
            PageBreak(),
            Paragraph("Analysis of Device Movement", self.styles["Heading1"]),
        ]
        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, movement_analysis)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            movement_analysis = movement_analysis.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in movement_analysis.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.styles["BodyText"])]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
            
        pages_content.extend([
        Image('plot.png', width=8*self.inch, height=5*self.inch), 
        Spacer(1, 0.5*self.inch)  
        ])
        for report in re.split('[*•#]', patterns_links):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.styles["BodyText"])]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
        pages_content.append(PageBreak())
        pages_content.append(Paragraph("Common Location Descriptions", self.styles["Heading1"]))
        for report in re.split('[*•#]', common_location):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.styles["BodyText"])]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
        
        pages_content.append(PageBreak())
        pages_content.extend([
        Paragraph("Device Co-location Analysis", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{device_colocation}", self.styles["BodyText"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph("Significance of Locations", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{significane_location}", self.styles["BodyText"]),
        PageBreak(),
        Paragraph("Conclusion", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{conclusion}", self.styles["BodyText"])
        ])



        
        # Add unique bookmark names for clickable links in the TOC
        for i, flowable in enumerate(pages_content):
            if isinstance(flowable, Paragraph) and flowable.style.name in ['Heading1', 'Heading2']:
                bn = sha1((flowable.getPlainText() + flowable.style.name).encode('utf-8')).hexdigest()
                pages_content[i] = Paragraph(flowable.getPlainText() + '<a name="%s"/>' % bn, flowable.style)
                pages_content[i]._bookmarkName = bn

        doc.multiBuild(pages_content)
        print(f"Professional PDF template generated successfully: {self.filename}")
    

# Example usage
generator = ReportPDFGenerator("Bd.pdf")
generator.generate_pdf(movement_analysis)

