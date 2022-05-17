# Based on:
# https://jamesthorne.co.uk/blog/processing-wikipedia-in-a-couple-of-hours/

import xml.sax


class WikiReader(xml.sax.ContentHandler):
    """
    Custom SAX ContentHandler. that reads Wikipedia articles from a Wikipedia
    dump (XML stream, dumps.wikimedia.org)

    """

    def __init__(self, ns_filter, article_callback):
        """
        Constructor

        Arguments:
        :param ns_filter: Filtering function for namespace (namespace 0 = articles)
        :param article_callback: Callback function for article processing,
            called with a tuple (title, article_text) for each article
        """
        super().__init__()

        self.filter = ns_filter

        self.read_stack = []
        self.read_text = None
        self.read_title = None
        self.read_namespace = None
        self.read_redirect = None

        self.status_count = 0
        self.callback = article_callback

    def startElement(self, tag_name, attributes):
        if tag_name == "ns":
            self.read_namespace = None

        elif tag_name == "page":
            self.read_text = None
            self.read_title = None
            self.read_redirect = None

        elif tag_name == "title":
            self.read_title = ""

        elif tag_name == "text":
            self.read_text = ""

        elif tag_name == "redirect":
            self.read_redirect = attributes.getValue("title")
            return

        else:
            return

        self.read_stack.append(tag_name)

    def endElement(self, tag_name):
        if len(self.read_stack) == 0:
            return

        if tag_name == self.read_stack[-1]:
            del self.read_stack[-1]

        if self.filter(self.read_namespace):
            if tag_name == "page" and self.read_text is not None:
                self.status_count += 1
                self.callback((self.read_title, self.read_text, self.read_redirect))

    def characters(self, content):
        if len(self.read_stack) == 0:
            return

        if self.read_stack[-1] == "text":
            self.read_text += content

        if self.read_stack[-1] == "title":
            self.read_title += content

        if self.read_stack[-1] == "ns":
            self.read_namespace = int(content)