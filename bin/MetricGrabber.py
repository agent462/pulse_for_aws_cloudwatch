## Author:: Andrew Painter (andrew.painter@bestbuy.com) 
## 
## Copyright 2012, BBY Solutions, Inc. 
## 
## Licensed under the Apache License, Version 2.0 (the "License"); you may not 
## use this file except in compliance with the License. You may obtain a copy 
## of the License at
## 
## http://www.apache.org/licenses/LICENSE-2.0 
## 
## Unless required by applicable law or agreed to in writing, software distributed 
## under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR 
## CONDITIONS OF ANY KIND, either express or implied. See the License for the 
## specific language governing permissions and limitations under the License.

import boto 
import boto.ec2.cloudwatch
import datetime
import time
import sys
from dateutil import tz
import splunk.entity as entity

class MetricGrabber:
			
	# AWS Access Key ID
	accesskey = None
	
	# AWS Secret Key
	secretkey = None
	
	def __init__(self):
		"""
		Init method to retrieve the AWS Access Key/Secret Key, setup the AWS Connection and Parse the CLI Arguments
		"""
		
		# Grab the Session ID Passed by Splunk.  Grab the secretkey and accesskey provided at setup
		self.grabKeys()
		self.endTime = datetime.datetime.utcnow()
		
		# Parser for passed arguments.  Allows formats of "region seconds" "seconds" "region" "seconds region" or none
		if len(sys.argv) == 2 and sys.argv[1].isdigit():
			self.region = "us-east-1"
			self.startTime = self.endTime - datetime.timedelta(seconds=int(sys.argv[1]))
		elif len(sys.argv) == 2 and not sys.argv[1].isdigit():
			self.region = sys.argv[1]
			self.startTime = self.endTime - datetime.timedelta(seconds=300)
		elif len(sys.argv) == 3:
			if sys.argv[1].isdigit():
				self.startTime = self.endTime - datetime.timedelta(seconds=int(sys.argv[1]))
				self.region = sys.argv[2]
			else:
				self.startTime = self.endTime - datetime.timedelta(seconds=int(sys.argv[2]))
				self.region = sys.argv[1]
		else:
			self.region = "us-east-1"
			self.startTime = self.endTime - datetime.timedelta(seconds=300)
		
		# Self imposed rate limit so we do not get throttled.  
		# The script allows this many requests per "secondsToSleep".  
		# If 5 different scripts run at the same time, we get 5*maxRequestsPerTimePeriod requests per secondsToSleep.
		self.maxRequestsPerTimePeriod = 2
		self.secondsToSleep = 1
		self.totalRequests = 0
		self.requestDateCounter = datetime.datetime.now() + datetime.timedelta(seconds=self.secondsToSleep)
		
		# Local Time
		self.tzinfo = tz.tzlocal()
		
		# Cloudwatch Connection to the specific region
		self.cloudwatch = boto.ec2.cloudwatch.connect_to_region(self.region, aws_access_key_id=self.accesskey, aws_secret_access_key=self.secretkey)
			
	def connect(self, namespace, queryType=None, attribute=None):
		"""
		Determines the type of request and either grabs the metrics or alarm history
		
		:type namespace: string
		:param namespace: The Metric Namespace we are retrieving.  AWS/ELB, AWS/EC2, AWS/EBS, etc.
		
		:type queryType: string
		:param queryType: Optional Metric name we would like to grab.  Can be a entire name (NetworkIn, NetworkOut)
						  or a partial match (Network)
		
		:type attribute: string
		:param attribute: Optional dimension to retrieve for the specified namespace and/or queryType.
						  These are found on the Cloudwatch console.  InstanceId, DBClass, EngineName, etc.
		"""
		
		self.namespace = namespace
		self.queryType = queryType
		self.attribute = attribute
		if self.namespace != "AlarmHistory":
			self.grabMetrics()
		else:
			self.grabAlarmHistory()
				
	def printMetrics(self, instanceId, i):
		"""
		Given a metric (i), grab the statistics by minute and print the metric results.
		"""
		if instanceId != None:

			# Increase how many requests we are doing so we can impose rate limits
			self.totalRequests += 1

			if (self.requestDateCounter > datetime.datetime.now()) and (self.totalRequests > self.maxRequestsPerTimePeriod):
				time.sleep(self.secondsToSleep)
				self.totalRequests = 0
				self.requestDateCounter = datetime.datetime.now() + datetime.timedelta(seconds=self.secondsToSleep)
			
			metricName = str(i).partition(":")[2]
			
			# MetricName -- Grab the "Total" aggregate metrics
			if self.attribute == "MetricName":
				values = self.cloudwatch.get_metric_statistics(60,self.startTime, self.endTime, metricName, self.namespace, i.Statistics)
			else:
				values = self.cloudwatch.get_metric_statistics(60,self.startTime, self.endTime, metricName, self.namespace, i.Statistics, dimensions = {self.attribute : instanceId})

			statsString = "%-25s%-35s" % ("Timestamp-UTC","Timestamp-Local")
		
			for stat in i.Statistics:
				counter = 0
				statsString += "%-25s" % stat
		
			for v in sorted(values, key=lambda val: val['Timestamp']):
				if v:
					print ""
					print "Group: %s -- Metric: %s -- Type: %s -- Region: %s" % (instanceId[0],metricName,self.attribute, self.region)
					print statsString
					tmp = "%-25s%-35s" % (v['Timestamp'].strftime("%m/%d/%Y %H:%M:%S"), self.tzinfo.fromutc(v['Timestamp'].replace(tzinfo=self.tzinfo)).strftime("%m/%d/%y %I:%M:%S.000 %p"))
					for stat in i.Statistics:
						tmp += "%-25s" % v[stat]
					print tmp
				
	def grabMetrics(self):
		"""
		Given a namespace, it will grab a list of metrics that match that namespace
		"""
		metrics = self.cloudwatch.list_metrics(None,None,None,self.namespace)

		while True:
			
			for i in metrics:
				
		  		instanceId = None
				
				if self.attribute and i.dimensions.get(self.attribute):
					instanceId = i.dimensions.get(self.attribute)
				elif self.attribute == "MetricName" and len(i.dimensions) == 0:
					instanceId = ["Total"]
			
				if instanceId:
					if self.queryType:
						if str(i).startswith("Metric:" + self.queryType):
							self.printMetrics(instanceId, i)
					else:
						self.printMetrics(instanceId, i)
							
			if metrics.next_token:
				metrics = self.cloudwatch.list_metrics(metrics.next_token)
			else:
				break
	
	def grabAlarmHistory(self):
		"""
		Gets a list of alarms that have triggered given the start and end time.
		"""
		alarms = self.cloudwatch.describe_alarm_history(None,self.startTime, self.endTime)
		
		while True:
			
			for a in alarms:
				print ""
				print "Alarm Name: %s -- Item Type: %s" % (a.name, a.tem_type)
				print "%-35s%-35s %s" % ("Timestamp-UTC","Timestamp-Local","Summary")
				print "%-35s%-35s %s" % (a.timestamp.strftime("%m/%d/%Y %H:%M:%S.%f"),  self.tzinfo.fromutc(a.timestamp.replace(tzinfo=self.tzinfo)).strftime("%m/%d/%y %I:%M:%S.%f %p"),a.summary)
				print "Raw Data: %s" % (a.data)
				print ""
				
			if alarms.next_token:
				alarms = self.cloudwatch.describe_alarm_history(None, self.startTime, self.endTime, None, None, alarms.next_token)
			else:
				break
	
	def getCredentials(self,sessionKey):
		"""
		Splunk provided code for grabbing username/passwords during setup.
		"""
		myapp = 'pulse_for_aws_cloudwatch'
		try:
		# list all credentials
			entities = entity.getEntities(['admin', 'passwords'], namespace=myapp,
			                              owner='nobody', sessionKey=sessionKey)
		except Exception, e:
			raise Exception("Could not get %s credentials from splunk. Error: %s"
			                % (myapp, str(e)))

		# return first set of credentials
		for i, c in entities.items():
			return c['username'], c['clear_password']

		raise Exception("No credentials have been found")

	def grabKeys(self):
		"""
		Splunk provided code to get the session key passed when executed.
		"""
		if self.accesskey == None:
			sessionKey = sys.stdin.readline().strip()

			if len(sessionKey) == 0:
			   sys.stderr.write("Did not receive a session key from splunkd. " +
			                    "Please enable passAuth in inputs.conf for this " +
			                    "script\n")
			   exit(2)

			self.accesskey, self.secretkey = self.getCredentials(sessionKey)
