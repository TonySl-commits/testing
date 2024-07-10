from reportlab.lib.pagesizes import letter

from reportlab.platypus import BaseDocTemplate



# Define your document template
class SimpleDocTemplate(BaseDocTemplate):
    def __init__(self, filename, **kw):
        self.allowSplitting = 0
        BaseDocTemplate.__init__(self, filename, **kw)
        self.pagesize = letter

    #called after every flowable (element) is added to the document to do table of contents
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