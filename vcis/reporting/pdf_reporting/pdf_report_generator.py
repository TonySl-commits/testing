"""
PDF Report Generator. This work is courtesy of Celine Rahme, who had the initial version of AI generated report.
Our edit on the initial version was to make the report generation dynamic with respect to the devices present in the AOI reporting.
This model is supposed to run in parallel with the HTML report generation engine and rely on the aoi_report_functions results.

This module is responsible for generating pdf reports based on the Groq queryresults and the pdf template.
The Groq query results are generated from the GroqModel class.
The pdf template is a BaseDocTemplate class from the ReportLab library.
The report is generated using the BaseDocTemplate.build() method.
The generated pdf is saved using the save() method of the BaseDocTemplate class.
The save path and the pdf template are passed as an argument to the generate_report()
method of the ReportPDFGenerator class.

This approach will allow a request per topic per device to be sent to the groq model.
In the case that a single request approach was needed then the architecture should be changed.
The propsed change was looping per device only and generating a single request to the groq model.
The model's answer then should be in a form that can be split in order to add the corresponding figure underneath.
"""


from reportlab.lib.pagesizes import letter
from reportlab.lib import colors, units
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from hashlib import sha1
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, NextPageTemplate, PageBreak, Image
from vcis.reporting.pdf_reporting.groq_response_generator import GroqModel
from vcis.utils.properties import CDR_Properties

class ReportPDFGenerator:
    def __init__(self, filename):
        self.properties = CDR_Properties()
        self.title = 'AOI Report'
        self.logo_path = self.properties.passed_filepath_reports_png + 'valoores.png'
        self.cover_image= self.properties.passed_filepath_reports_png + 'cover_page.jpg'
        self.abstract = ""
        self.filename = self.properties.passed_filepath_reports_results + filename
        self.inch = units.inch
        self.styles = getSampleStyleSheet()
        self.setup_styles()

    def generate_text(self, system, text):
        groq_model = GroqModel(system)
        response = groq_model.generate_response(text) 

    def set_logo(self, path:str):
        self.logo_path = path

    def set_image(self, path:str):
        self.image = path

    def set_abstract(self, text:str):
        self.abstract = text

    def setup_styles(self):

        body_style = self.styles['BodyText']
        body_style.fontSize = 11
        body_style.textColor= colors.HexColor('#002147')
        body_style.leading = 30

        bullet_style = self.styles['BodyText']
        bullet_style.fontSize = 11
        bullet_style.leading = 30

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

    def create_paragraph(self,title, content, style:int):
        style = "Heading1" if style == 1 else "Heading2"
        list_to_add = [
        Paragraph(f"{title}", self.styles[style]),
        Spacer(0.5, 0.05*self.inch),
        Paragraph(f"{content}", self.styles["BodyText"]),
        Spacer(1, 0.05*self.inch)
        ]
        return list_to_add
    
    def generate_pdf(self, device_list):
        doc = SimpleDocTemplate(self.filename, pagesize=letter)
        frameT = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')

        first_page_template = PageTemplate(id='FirstPage', frames=frameT, onPage=self.add_page_number_and_footer)
        later_page_template = PageTemplate(id='Later', frames=frameT, onPage=self.add_page_number_and_footer)

        doc.addPageTemplates([first_page_template, later_page_template, PageTemplate(id='ContentPage', frames=frameT)])

        toc = self.create_toc()

        pages_content = [
            Image(self.logo_path, width=1.7*self.inch, height=1*self.inch, hAlign="CENTER"),
            Paragraph(self.title, ParagraphStyle( "TitleStyle",fontSize=50,textColor=colors.HexColor('#002147'),alignment=TA_LEFT, spaceAfter=50,spaceBefore=-0.05 )),
            Spacer(0.5, 0.05*self.inch),
            Paragraph(f"{self.abstract}", ParagraphStyle(name='body', fontSize=10, textColor=colors.HexColor('#002147'), fontName='Times-Italic', leading=15)),
            Spacer(1, 0.5*self.inch),
            Image(self.cover_image, width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),

            NextPageTemplate('ContentPage'),
            PageBreak(),
            Paragraph('<b>Table of Contents</b>', ParagraphStyle(name='centered', fontSize=20, textColor=colors.HexColor('#002147'), leading=16, alignment=1, spaceAfter=20)),
            Spacer(1, 0.5*self.inch),
            toc
        ]

        # this loops over all the devices in the device list
        for device in device_list:
            pages_content.append(PageBreak())
            for key, value in device.items():
                pages_content.append(Paragraph(f"{key}", self.styles["Heading1"]))
                if type(value) == dict:
                    for k, v in value.items():
                        if v['system'] == '' or v['user'] == '':
                            continue
                        model = GroqModel(v['system'])
                        response = model.generate_response(v['user'])
                        for item in self.create_paragraph(f"{k}", response, 2):
                            pages_content.append(item)
                        if v['figure'] is not None and v['figure'] != '': 
                            pages_content.append(Image(v['figure'], hAlign="CENTER"),)

        for item in self.create_paragraph("Conclusion", "",1):
            pages_content.append(item)

        # Add unique bookmark names for clickable links in the TOC
        for i, flowable in enumerate(pages_content):
            if isinstance(flowable, Paragraph) and flowable.style.name in ['Heading1', 'Heading2']:
                bn = sha1((flowable.getPlainText() + flowable.style.name).encode('utf-8')).hexdigest()
                pages_content[i] = Paragraph(flowable.getPlainText() + '<a name="%s"/>' % bn, flowable.style)
                pages_content[i]._bookmarkName = bn

        doc.multiBuild(pages_content)

        print(f"Professional PDF template generated successfully: {self.filename}")

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