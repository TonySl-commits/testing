from vcis.utils.properties_ai import AIProperties
from vcis.utils.properties import CDR_Properties

from reportlab.lib.pagesizes import letter
from reportlab.lib import colors, units
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_JUSTIFY
from hashlib import sha1
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, NextPageTemplate, PageBreak, Image
from reportlab.lib import colors
from reportlab.platypus import Table, TableStyle
from reportlab.lib.units import inch

import re

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

class SimulationReportLayout():
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.properties_ai = AIProperties()
        self.properties = CDR_Properties()
        self.filename = self.properties_ai.deafualt_pdf_file_name
        self.inch = units.inch
        self.styles = getSampleStyleSheet()
        self.setup_styles()
        self.justified_style = self.styles["BodyText"].clone('justified_style')
        self.justified_style.alignment = TA_JUSTIFY 

   
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
    
    
    def generate_pdf_dh(self,title,table_id, abstract, introduction, summary_table, movement_analysis, patterns_links, common_location, device_colocation, significane_location, conclusion, mapping_table):
        doc = SimpleDocTemplate(self.filename.format(table_id), pagesize=letter)
        frameT = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')
        
        first_page_template = PageTemplate(id='FirstPage', frames=frameT, onPage=self.add_page_number_and_footer)
        later_page_template = PageTemplate(id='Later', frames=frameT, onPage=self.add_page_number_and_footer)
        
        doc.addPageTemplates([first_page_template, later_page_template, PageTemplate(id='ContentPage', frames=frameT)])
        
        toc = self.create_toc()
        
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
        col_widths = [3*inch, 3*inch]
        mapping_table = Table(mapping_table, colWidths=col_widths,repeatRows=1)
        mapping_table.setStyle(style)
        summary_table.setStyle(style)

        pages_content = [
            Image(self.properties.report_valoores_png_path, width=1.7*self.inch, height=1*self.inch, hAlign="LEFT"),
            Paragraph(title, ParagraphStyle( "TitleStyle",fontSize=50,textColor=colors.HexColor('#002147'),alignment=TA_LEFT, spaceAfter=50,spaceBefore=-0.05 )),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{abstract}", ParagraphStyle(name='body', fontSize=10, textColor=colors.HexColor('#002147'), fontName='Times-Italic',alignment=TA_JUSTIFY, leading=15)),
            Spacer(1, 0.5*self.inch), 
            Image(self.properties.passed_filepath_reports_png +'intro_image.jpg', width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),
            
            NextPageTemplate('ContentPage'),
            PageBreak(),
            Paragraph('<b>Table of Contents</b>', ParagraphStyle(name='centered', fontSize=20, textColor=colors.HexColor('#002147'), leading=16, alignment=1, spaceAfter=20)),
            Spacer(1, 0.5*self.inch), 
            toc,
            PageBreak(),
            
            Image(self.properties.passed_filepath_reports_png +'simulation_image_1.png', width=9*self.inch, height=4*self.inch, hAlign="CENTER"),
            Spacer(1, 0.05*self.inch),
            Paragraph("Introduction", self.styles["Heading1"]),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{introduction}", self.justified_style),
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
            
            report_paragraphs = [Paragraph(f"• {report}", self.justified_style)]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
            
        pages_content.extend([
        Image(self.properties.passed_filepath_reports_png + 'plot.png', width=8*self.inch, height=5*self.inch), 
        Spacer(1, 0.5*self.inch)  
        ])
        for report in re.split('[*•#]', patterns_links):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.justified_style)]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
        pages_content.append(PageBreak())
        pages_content.append(Paragraph("Common Location", self.styles["Heading1"]))
        pages_content.append(Spacer(1, 0.05*self.inch))
        pages_content.append(Paragraph("Description", self.styles["Heading2"]))
        for report in re.split('[*•#]', common_location):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.justified_style)]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
        
        pages_content.extend([
        Image(self.properties.passed_filepath_reports_png +'plot2.png', width=6*self.inch, height=3.*self.inch, hAlign="CENTER"),
        Spacer(1, 0.05*self.inch),
        PageBreak(),
        Paragraph("Device Co-location Analysis", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{device_colocation}", self.justified_style),
        Spacer(1, 0.05*self.inch), 
        Paragraph("Significance of Locations", self.styles["Heading2"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{significane_location}", self.justified_style),
        PageBreak(),
        Paragraph("Conclusion", self.styles["Heading2"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{conclusion}", self.justified_style),
        PageBreak(),
        Spacer(1, 0.05*self.inch), 
        Paragraph("DeviceID Mapping Table", self.styles["Heading1"]),
        Spacer(1, 0.1*self.inch), 
        mapping_table,
        ])



        
        # Add unique bookmark names for clickable links in the TOC
        for i, flowable in enumerate(pages_content):
            if isinstance(flowable, Paragraph) and flowable.style.name in ['Heading1', 'Heading2']:
                bn = sha1((flowable.getPlainText() + flowable.style.name).encode('utf-8')).hexdigest()
                pages_content[i] = Paragraph(flowable.getPlainText() + '<a name="%s"/>' % bn, flowable.style)
                pages_content[i]._bookmarkName = bn

        doc.multiBuild(pages_content)
        print(f"Professional PDF template generated successfully: {self.filename.format(table_id)}")

######################################################################################################################################
    
    def generate_pdf_ac(self,
                        title, 
                        table_id, 
                        abstract, 
                        introduction, 
                        summary_table, 
                        activity_scan_hits_distribution, 
                        conclusion, 
                        mapping_table, 
                        timespent_analysis, 
                        timespent_insights_observation
                        ):
        doc = SimpleDocTemplate(self.filename.format(table_id), pagesize=letter)
        frameT = Frame(doc.leftMargin, doc.bottomMargin, doc.width, doc.height, id='normal')
        
        first_page_template = PageTemplate(id='FirstPage', frames=frameT, onPage=self.add_page_number_and_footer)
        later_page_template = PageTemplate(id='Later', frames=frameT, onPage=self.add_page_number_and_footer)
        
        doc.addPageTemplates([first_page_template, later_page_template, PageTemplate(id='ContentPage', frames=frameT)])
        
        toc = self.create_toc()
        
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
        col_widths = [3*inch, 3*inch]
        mapping_table = Table(mapping_table, colWidths=col_widths,repeatRows=1)
        mapping_table.setStyle(style)
        summary_table.setStyle(style)

        pages_content = [
            Image(self.properties.report_valoores_png_path, width=1.7*self.inch, height=1*self.inch, hAlign="LEFT"),
            Paragraph(title, ParagraphStyle( "TitleStyle",fontSize=50,textColor=colors.HexColor('#002147'),alignment=TA_LEFT, spaceAfter=50,spaceBefore=-0.05 )),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{abstract}", ParagraphStyle(name='body', fontSize=10, textColor=colors.HexColor('#002147'), fontName='Times-Italic',alignment=TA_JUSTIFY, leading=15)),
            Spacer(1, 0.5*self.inch), 
            Image(self.properties.passed_filepath_reports_png +'intro_image.jpg', width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),
            
            NextPageTemplate('ContentPage'),
            PageBreak(),
            Paragraph('<b>Table of Contents</b>', ParagraphStyle(name='centered', fontSize=20, textColor=colors.HexColor('#002147'), leading=16, alignment=1, spaceAfter=20)),
            Spacer(1, 0.5*self.inch), 
            toc,
            PageBreak(),
            
            Image(self.properties.passed_filepath_reports_png +'simulation_image_1.png', width=9*self.inch, height=4*self.inch, hAlign="CENTER"),
            Spacer(1, 0.05*self.inch),
            Paragraph("Introduction", self.styles["Heading1"]),
            Spacer(0.5, 0.05*self.inch), 
            Paragraph(f"{introduction}", self.justified_style),
            Spacer(1, 0.05*self.inch),
            summary_table,
            PageBreak(),
            Paragraph("Analysis of The Acivity Scan Hits Distribution DOW", self.styles["Heading1"]),
            Spacer(1, 0.05*self.inch),
            Image(self.properties.passed_filepath_reports_png +'dow_image.png', width=9*self.inch, height=4.5*self.inch, hAlign="CENTER"),
            Spacer(1, 0.05*self.inch),
        ]

        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, activity_scan_hits_distribution)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            activity_scan_hits_distribution = activity_scan_hits_distribution.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in activity_scan_hits_distribution.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.justified_style)]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))
        pages_content.extend([PageBreak(),
                              Paragraph("TimeSpent Analysis at AOI", self.styles["Heading1"]),
                              Spacer(1, 0.05*self.inch),])

        
        header_pattern = r'\*\*(.*?)\*\*'
        headers = re.findall(header_pattern, timespent_analysis)
        for header in headers:
            placeholder = f'<b>{header}</b>'
            timespent_analysis = timespent_analysis.replace(f'**{header}**', placeholder)
        
         # Split the reports by new lines and iterate over them
        for report in timespent_analysis.split('\n\n'):
            # Skip empty lines
            if not report.strip():
                continue
            
                # Split each report into report number and content
            report_lines = report.split('\n')
            pages_content.append(Spacer(0.5, 0.05*self.inch))
            
            # Add the report content as bullet points
            
            report_paragraphs = [Paragraph(f"• {report}", self.justified_style)]
            pages_content.extend(report_paragraphs)
            pages_content.append(Spacer(1, 0.05*self.inch))

        pages_content.append(PageBreak())
        pages_content.extend([
        Paragraph("Insights and Observations", self.styles["Heading2"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{timespent_insights_observation}", self.justified_style),
        PageBreak(),
        ])
        pages_content.extend([
        Paragraph("Conclusion", self.styles["Heading1"]),
        Spacer(1, 0.05*self.inch), 
        Paragraph(f"{conclusion}", self.justified_style),
        PageBreak(),
        Spacer(1, 0.05*self.inch), 
        Paragraph("DeviceID Mapping Table", self.styles["Heading1"]),
        Spacer(1, 0.1*self.inch), 
        mapping_table,
        ])



        
        # Add unique bookmark names for clickable links in the TOC
        for i, flowable in enumerate(pages_content):
            if isinstance(flowable, Paragraph) and flowable.style.name in ['Heading1', 'Heading2']:
                bn = sha1((flowable.getPlainText() + flowable.style.name).encode('utf-8')).hexdigest()
                pages_content[i] = Paragraph(flowable.getPlainText() + '<a name="%s"/>' % bn, flowable.style)
                pages_content[i]._bookmarkName = bn

        doc.multiBuild(pages_content)
        print(f"Professional PDF template generated successfully: {self.filename.format(table_id)}")