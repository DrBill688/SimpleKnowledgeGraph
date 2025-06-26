# -*- coding: utf-8 -*-
"""
@author: William N. Roney, ScD
"""

import sys
sys.path.append('../..')

import logging
FORMAT = "[<%(name)s> %(asctime)s:%(filename)s:%(lineno)s:%(levelname)s] %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)
logger =  logging.getLogger(__name__)

from SimpleKnowledgeGraph import SimpleKnowledgeGraph, SKGDeclaration
import pandas as pd

print('Starting test case for SimpleKnowledgeGraph.')
skg = SimpleKnowledgeGraph()

def xformDate(orig):
    import datetime
    dt = datetime.datetime.strptime(orig, '%m/%d/%Y').date()
    return dt.strftime('%Y%m%d')
def xformUpperCase(orig):
    return orig.upper()
def xformBool(orig):
    return "TRUE" if orig else "FALSE"


df = pd.read_csv('milestones.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='Milestone', column='MilestoneName', primary_key=True, description='This is the name of a milestone.') \
          .node(label='Target Date', column='TargetDate', description='This is the desired delivery date of the milestone.', transform_function=xformDate) \
          .node(label='Reported Status', column='ReportedStatus', description='This is the PM subjective status.', transform_function=xformUpperCase) \
          .node(label='Portfolio', column='Portfolio', description='A grouping for milestones.') \
          .node(label='Portfolio Group', column='PortfolioGroup', description='A grouping for portfolios.') \
          .node(label='Market Segment', column='MarketSegment', description='A grouping for milestones.') \
          .node(label='Board Commitment', column='ShowBoard', description='Should this appear for the Board of Directors.', transform_function=xformBool) \
          .node(label='Milestone Description', column='Description', description='A description of the milestones purpose.') \
          .node(label='Work ID', column='WorkID', description='This is a link to the work.') \
          .node(label='Distribution Program', column='DistributionProgram', description='A grouping for milestones.') 

df = pd.read_csv('work.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='Work ID', column='WorkID', primary_key=True, description='This is linkage field for work to other entities.') \
          .node(label='Work Status', column='WorkStatus', description='This is the subjective status of the work.') \
          .node(label='Work Target Start Date', column='WorkTargetStart', description='This is planned starting date for this work item.', transform_function=xformDate) \
          .node(label='Work Description', column='WorkDescription', description='A description of what this work enables.') \
          .node(label='Work Name', column='WorkName', description='The preferred alias for Work ID for human consumption.') \
          .node(label='PID', column='PID', description='An alias for Work ID used by some entities.') \
          .node(label='Department', column='Department', description='A grouping for work.') \
          .node(label='Value Stream', column='ValueStream', description='A grouping for work.') 

df = pd.read_csv('party.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='ATTUID', column='UID', primary_key=True, description='This is linkage field for work to other entities.') \
          .node(label='Display Name', column='DisplayName', description='The preferred alias for ATTUID for human consumption.') \
          .node(label='Other ID', column='MSGraphID', description='This an alias for ATTUID used for linking to other systems.') 
          
df = pd.read_csv('interestedparties.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='Role', column='Role', primary_key=True, description='Role this person plays to a milestone.') \
          .node(label='Milestone Name', column='MilestoneName', primary_key=True, description='Linkage to Milestone.') \
          .node(label='ATTUID', column='ATTUID', description='Linkage to Person.')
          
df = pd.read_csv('application.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='Application ID', column='AppID', primary_key=True, description='.') \
          .node(label='ITAP Acronym', column='ITAPAcronym', description='.') \
          .node(label='ITAP Lifecycle', column='ITAPLifecycle', description='.') \
          .node(label='ITAP Owner', column='ITAPOwner', description='.') 
          
df = pd.read_csv('appimpact.csv')
for _,record in df.iterrows():
    with SKGDeclaration(skg, record) as tc:
        tc.node(label='Work ID', column='WorkID', primary_key=True, description='.') \
          .node(label='Application ID', column='AppID', primary_key=True, description='.') \
          .node(label='Application Estimate', column='ImpactDollars', description='.') 
          
print('*****************Model****************')          
skg.reloadModel()
print(skg.verbalizeModel())
print('*****************Database****************')          
print(str(skg))
print('*****************Query****************')          
print('Milestones(Target Date) where "Reported Status" eq "Green"\nTarget Date between 20250701 and 20251231\nWork ID == 1006628\nBoard Commitment == TRUE\nApplication Estimate == 1')
result = skg.query() \
          .perspective('Milestone') \
          .include(['Target Date', 'Work Name']) \
          .add_restriction('or','Reported Status', 'eq', 'GREEN') \
          .add_restriction('and','Target Date', 'lt', '20251231') \
          .add_restriction('and','Target Date', 'gt', '20250601') \
          .add_restriction('or','Work ID', 'eq', '1006628') \
          .add_restriction('or','Board Commitment', 'eq', 'TRUE') \
          .add_restriction('or','Application Estimate', 'eq', '1') \
          .result()
import json
print(json.dumps(result, indent=4))            

print('Ending test case for SimpleKnowledgeGraph.')
