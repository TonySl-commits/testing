from reportlab.lib import colors, units
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from hashlib import sha1
from reportlab.lib.pagesizes import letter
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus import  BaseDocTemplate,PageTemplate, Frame, Paragraph, Spacer, NextPageTemplate, PageBreak, Image
from vcis.reporting_pdf.case_reporting.doctemplate import SimpleDocTemplate
from vcis.utils.utils import CDR_Properties
from vcis.utils_AI.properties_ai import AI_Properties
from vcis.utils_AI.utils_ai import AI_Utils
from vcis.reporting_pdf.case_reporting.case_report_generator import CaseReportGenerator

    
class ReportPDFGenerator:
    
    def __init__(self, filename):
        self.filename = filename
        self.inch = units.inch
        self.styles = getSampleStyleSheet()
        self.properties = AI_Properties()
        self.utils=AI_Utils()
        self.simpleDocTemplate=SimpleDocTemplate()
        self.setup_styles()
        # Title
        self.title = "Case Report"
        # Logo
        self.logo_path = "src/cdr_trace/reporting_pdf/report_main_pdf/logo.png"

        self.image="src/cdr_trace/reporting_pdf/report_main_pdf/tele.jpg"
        self.abstract=self.properties.abstract_case_template
        self.case_report_generator = CaseReportGenerator()
   
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
        
        heading3_style = self.styles['Heading3']
        heading3_style.fontSize = 12
        heading3_style.textColor =  colors.HexColor('#002147')
       
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
            # Remove the asterisk from the start of each line
            cleaned_point = point.replace('*', '').strip()
            if cleaned_point:
                bullet_content.append(Paragraph(f"• {cleaned_point}", self.styles['BodyText']))
    
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
    ###here later we will ad the ai conclusion
    
    def generate_pdf(self,intro_text,reports):
        doc = SimpleDocTemplate(self.filename, pagesize=letter)
        frameT = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')
        
        first_page_template = PageTemplate(id='FirstPage', frames=frameT, onPage=self.add_page_number_and_footer)
        later_page_template = PageTemplate(id='Later', frames=frameT, onPage=self.add_page_number_and_footer)
        
        doc.addPageTemplates([first_page_template, later_page_template, PageTemplate(id='ContentPage', frames=frameT)])
        
        toc = self.create_toc()
        
        pages_content = [
            Image(self.logo_path, width=1.7*self.inch, height=1*self.inch, hAlign="LEFT"),
            Paragraph(f"{self.title}", ParagraphStyle( "TitleStyle",fontSize=50,textColor=colors.HexColor('#002147'),alignment=TA_LEFT, spaceAfter=50,spaceBefore=-0.05 )),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{self.abstract}", ParagraphStyle(name='body', fontSize=10, textColor=colors.HexColor('#002147'), fontName='Times-Italic', leading=15)),
            Spacer(1, 0.5*self.inch), 
            Image(self.image, width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),
            
            NextPageTemplate('ContentPage'),
            PageBreak(),
            Paragraph('<b>Table of Contents</b>', ParagraphStyle(name='centered', fontSize=20, textColor=colors.HexColor('#002147'), leading=16, alignment=1, spaceAfter=20)),
            Spacer(1, 0.5*self.inch), 
            toc,
            PageBreak(),
            
            Paragraph("Introduction", self.styles["Heading1"]),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{intro_text}", self.styles["BodyText"]),
            Spacer(1, 0.05*self.inch), 
            
            PageBreak(),
            Paragraph("Analysis Of Reports", self.styles["Heading1"]),
            
        ]
   

        # Split the reports by new lines and iterate over them
        for report in reports.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
            # Check if the current report is the case description
            if "Case Description:" in report:
                case_description_heading = Paragraph("Case Description", self.styles["Heading1"])
                pages_content.append(case_description_heading)
                pages_content.append(Spacer(0.5, 0.05*self.inch))
                
                # Split the case description from the heading and add it as content
                case_description_content = report.replace("Case Description:", "").strip()
                case_description_paragraph = Paragraph(case_description_content, self.styles["BodyText"])
                pages_content.append(case_description_paragraph)
                pages_content.append(Spacer(1, 0.05*self.inch))
                pages_content.append(PageBreak())
                
            else:
                # Split each report into report number and content
                report_lines = report.split('\n')
                report_number, report_content = report_lines[0], '\n'.join(report_lines[1:])
                
                # Add the report heading
                report_heading = Paragraph(report_number, self.styles["Heading2"])
                pages_content.append(report_heading)
                pages_content.append(Spacer(0.5, 0.05*self.inch))
                
                # Add the report content as bullet points
                bullet_points = self.add_bullet_points(report_content)
                pages_content.extend(bullet_points)
                pages_content.append(Spacer(1, 0.05*self.inch))

        pages_content.extend([
        Paragraph("Visual Representations", self.styles["Heading1"]),
        Spacer(1, 0.5*self.inch), 
        Image('bar_chart.png', width=3*self.inch, height=2*self.inch), 
        Spacer(1, 0.5*self.inch),
        Image('line_chart.png', width=3*self.inch, height=2*self.inch), 
        Spacer(1, 1*self.inch), 
        PageBreak(),
        
        Paragraph("Conclusion", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{self.properties.case_conclusion}", self.styles["BodyText"]),
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
generator.generate_pdf()