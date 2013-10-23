# Copyright 2009-2010 Yelp
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""An implementation of wc as an MRJob.

This is meant as an example of why mapper_final is useful."""
from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol
import re
def isNotWikiLink(aLink):
	start = 1;
	if aLink.startswith("[["):
		start = 2;
        if len(aLink)<(start+2) or  len(aLink)>100:
		return True;
	firstChar = aLink[start]
        
	if firstChar == '#': return True;
	if firstChar == ',': return True;
	if firstChar == '.': return True;
	if firstChar == '&': return True;
	if firstChar == '\'': return True;
	if firstChar == '-': return True;
	if firstChar == '{': return True;

	if aLink.find(":")>-1: return True;  # Matches: external links and translations links
	if aLink.find(",")>-1: return True; # Matches: external links and translations links
	if aLink.find("&")>-1: return True;
        return False

def sweetify(aLinkText):
	if aLinkText.find("&amp;")>-1:
            aLinkText.replace("&amp;", "&");

        return aLinkText;

def getWikiPageFromLink(aLink):
	if isNotWikiLink(aLink):
		return None
	if aLink.startswith("[["):
		start = 2 
	else:
		start =1
	endLink = aLink.find("]")

	pipePosition = aLink.find("|")
	if pipePosition > 0:
		endLink = pipePosition
	part = aLink.find("#")
        if part > 0:
		endLink = part
	
	aLink =  aLink[start:endLink]
	aLink = aLink.replace("\\s", "_")
	aLink = aLink.replace(",", "")
	aLink = sweetify(aLink)

	return aLink

class MRJob1(MRJob):
    INPUT_PROTOCOL = RawValueProtocol 
    def __init__(self, *args, **kwargs):
        super(MRJob1, self).__init__(*args, **kwargs)
    
    
    def mapper(self, _, line):
    	title = line[0:line.find("#")]
	text = line[line.find("#")+1:len(line)]
	#print "mapper number = ",title
	title = title.replace(' ', '_')	
	text = text.replace(' ', '_')
	links = re.findall( '\[\[.+?\]\]', text)
	for t in links:
		tt= getWikiPageFromLink(t)
		if tt!=None:
			yield(title,tt)				
			
	
    
    def reducer(self, key, values):
	pagerank ="1.0   "
	first = True
	for x in  values:
        	if not first:
			pagerank += ","
		try:
			pagerank += str(x)
		except:
			pagerank += x.encode("utf-8")
			pass
		first = False
	yield(key, pagerank)


if __name__ == '__main__':
    MRJob1.run()
