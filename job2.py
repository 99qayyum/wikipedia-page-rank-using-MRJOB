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

class MRJob2(MRJob):

    INPUT_PROTOCOL = RawValueProtocol 
    
    def __init__(self, *args, **kwargs):
        super(MRJob2, self).__init__(*args, **kwargs)
	pass 
    
    def mapper(self, _, line):	
	page=""
	i=1
	while(1):
		if  line[i]!="\"":
			page+=line[i]
		else:
			break
		i+=1
	i+=1
	while(line[i]!="\""):
		i+=1
	i+=1
	rank=""
	while (1):
		if  line[i]!=" ":
			rank+=line[i]
		else:
			break
		i+=1
	while(line[i]==" "):
		i+=1

	yield(page,"!")
	pages= line[i:len(line)]
	links= pages.split(",")
	total_links=len(links)
	for p in links:
		if len(p)<2:
			continue
		yield(p,page +"#"+rank+"#"+str(total_links))
	yield(page,"| "+pages)	
	
    
    def reducer(self, key, values):
	isExistingWikiPage = False
	split = []
	sumShareOtherPageRanks = 0;
	links = ""
	pageWithRank = ""
	
	for v in values:
		pageWithRank = v
		if v=="!":
			isExistingWikiPage = True
			continue

		if pageWithRank.startswith("|"):
			links = "\t"+pageWithRank[2:len(v)]
			continue
            	pageWithRank = pageWithRank.encode('ascii')
		split = pageWithRank.split("#")
		pageRank = split[1]
		countOutLinks = split[2];
	
		sumShareOtherPageRanks += (float(pageRank)/float(countOutLinks))
	
	if not isExistingWikiPage:
		return
	damping = 0.85
	newRank = damping * sumShareOtherPageRanks + (1-damping);
        yield(key,str(newRank)+"  "+links)
 

if __name__ == '__main__':
    MRJob2.run()
