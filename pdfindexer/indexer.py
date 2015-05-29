import os
from optparse import OptionParser
import re
import yapot
import magic
from elasticsearch import Elasticsearch

_DEBUG = False

def get_pdf_filenames(directory):

    """ gets a list of all the pdf files in the directory 
        using the magic library
    """
    filelist = [ f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory,f)) ]
    retlist = []
    for f in filelist:
        filename = os.path.join(directory,f)
        t = magic.from_file(filename, mime=True)
        if t == 'application/pdf':
            retlist.append(filename)
    return retlist

def convert_document(filename):

    """ converts a pdf document to text using yapot """
    success, pdf_text = yapot.convert_document(
        pdf_filename = filename,
        #resolution = 200,
        delete_files = False,
        page_delineation = '\n',
        verbose = _DEBUG,
        temp_dir = './.tmp',
        #make_thumbs = False,
        #thumb_size = None,
        #thumb_dir = None,
    )
    
    if _DEBUG == True:
        print "PDF Contents:\n\n"
        print pdf_text
        print "\n\n"

    text = None
    if success and pdf_text.strip() != '':
        text = pdf_text.strip()
        text.replace('\r','\n')
        for i in range(0,3):
            text = re.sub(' +',' ', text)
            text = re.sub('\t+',' ', text)
            text = re.sub('\n+',' ', text)
    return text

def index_document(server, index_name, contents):

    """ adds the document contents to elasticsearch """

    if _DEBUG == True:
        print "Indexing document with Elastic Search ..."

    es = Elasticsearch([server])

    res = es.index(
        index=index_name,
        doc_type='pdf',
        id=1,
        body={
            'contents': contents,
        },
    )

    if _DEBUG == True:
        print "Done indexing"

def index_directory(server, index_name, directory):

    """ passes all of the pdf documents in a directory
        into elastic search so they can be indexed
    """
    files = get_pdf_filenames(directory)

    for f in files:
        pdf_text = convert_document(f)
        if pdf_text != None:
            index_document(server, index_name, pdf_text)

    return len(files)

if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option("-d", "--target-directory", dest="target_dir",
        help="Target Directory to read PDFs from.", metavar="DIR")

    parser.add_option("-s", "--server", dest="server",
        help="Elastic Search Server", metavar="SERVER")

    parser.add_option("-i", "--index-name", dest="index_name",
        help="Name of the Elastic Search index.", metavar="INDEX")

    parser.add_option("-v", "--verbose", action="store_true",
        dest="verbose", help="Produce lots of debug output.",
        default=False)

    options, args = parser.parse_args() 

    _DEBUG = False
    if options.verbose == True:
        _DEBUG = True

    target_dir = '.'
    if not options.target_dir == '' and not options.target_dir == None:
        target_dir = options.target_dir
        if _DEBUG:
            print "Setting target directory to {0}".format(target_dir)

    server = 'http://127.0.0.1:9200'
    if not options.server == '' and not options.server == None:
        server = options.server
        if _DEBUG:
            print "Setting server to {0}".format(server)

    index_name = 'pdf_index'
    if not options.index_name == '' and not options.index_name == None:
        index_name = options.index_name
        if _DEBUG:
            print "Setting index name to {0}".format(index_name)

    if _DEBUG:
        print "INFO: Indexing PDFs in directory: {0}".format(target_dir)

    if os.path.exists(target_dir):
        count = index_directory(server, index_name, target_dir)

    if _DEBUG:
        print "INFO: Done indexing {0} pdfs.".format(count)

