
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition)
{
	    itr = NULL;
	    recordDescriptor.clear();
	    rm = RelationManager::instance();
	    itr = input;
	    cond.bRhsIsAttr = condition.bRhsIsAttr;
	    cond.lhsAttr = condition.lhsAttr;
	    cond.op = condition.op;
	    cond.rhsValue.data = NULL;
	    cond.rhsValue.data = condition.rhsValue.data;
	    //Ç³¿½±´
	    cond.rhsValue.type = condition.rhsValue.type;
	    cond.rhsAttr = condition.rhsAttr;
	    input->getAttributes(recordDescriptor);
}
RC Filter::getNextTuple(void* data)
{
	void* buffer = malloc(PAGE_SIZE);
	void* attrBuf = malloc(PAGE_SIZE);
	int fieldNum = recordDescriptor.size();
	int len;
	int offset;
	int offset1;
	int len1;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	while(itr->getNextTuple(buffer) != -1)
	{
		offset = 0;
		offset1 = 0;
		for(int i = 0; i < fieldNum; i++)
		{
				switch(recordDescriptor[i].type)
				{
				case TypeInt:
					len = sizeof(int);
					break;
				case TypeVarChar:
					len = *((int*) ((char*)buffer + offset)) + sizeof(int);
					break;
				case TypeReal:
					len = sizeof(float);
					break;
				}
				if(recordDescriptor[i].name == cond.lhsAttr)
				{
					offset1 = offset;
					len1 = len;
				}
				offset += len;
		}
		memcpy(attrBuf, (char*)buffer + offset1, len1);
		bool judge = rbfm->judgeAttribute(cond.rhsValue.data,attrBuf,cond.op,cond.rhsValue.type);
		if(judge == true)
		{
			memcpy(data, buffer, offset);
			free(buffer);
			free(attrBuf);
			return 0;
		}
	}
	free(buffer);
	free(attrBuf);
	return -1;
}
void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	itr->getAttributes(attrs);
};
Filter::~Filter()
{
	itr = NULL;
	rm = NULL;
	recordDescriptor.clear();
	cond.rhsValue.data = NULL;
};
// ... the rest of your implementations go here
Project::Project(Iterator *input, const vector<string> &attrNames)
{
	rm  = NULL;
	itr = NULL;
	itr = input;
	recordDescriptor.clear();
	projectAttribute.clear();
	rm = RelationManager::instance();
	itr->getAttributes(recordDescriptor);
	for( int i = 0; i < attrNames.size(); i++)
	{
		projectAttribute.push_back(attrNames[i]);
	}

};
RC Project::getNextTuple(void *data)
{
	int offset;
	int offset1;
	int len;
	AttrType colType;
	void *buffer = malloc(PAGE_SIZE);
	if(itr->getNextTuple(buffer) != -1)
	{
	  offset1=0;
	  for(int i = 0; i < projectAttribute.size(); i++)
	  {
	 			  offset = 0;//offset of projected attributes in record
	 			  for(int j = 0; j < recordDescriptor.size(); j++)
	 			  {
	 				  switch(recordDescriptor[j].type)
	 				  {
	 				  case TypeInt:
	 					  len = sizeof(int);
	 					  colType = TypeInt;
	 					  break;
	 				  case TypeVarChar:
	 					  len = *((int*) ((char*)buffer + offset)) + sizeof(int);
	 					  colType = TypeVarChar;
	 					  break;
	 				  case TypeReal:
	 					  len = sizeof(float);
	 					  colType = TypeReal;
	 					  break;
	 				  }
	 				  if(recordDescriptor[j].name == projectAttribute[i])
	 				  {
	 					  memcpy((char*)data + offset1, (char*)buffer + offset, len);
	 					  offset1 += len;
	 					  break;
	 				  }
	 				  offset += len;
	 			  }

	  }
	  free(buffer);
	  return 0;
	}
	free(buffer);
	return -1;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	 for(int i = 0; i < projectAttribute.size(); i++)
	{
		 			  //offset = 0;//offset of projected attributes in record
		 			  for(int j = 0; j < recordDescriptor.size(); j++)
		 			  {
		 				  if(recordDescriptor[j].name == projectAttribute[i])
		 				  {
		 					  Attribute attribute;
		 					  attribute.name = recordDescriptor[j].name;
		 					  attribute.length = recordDescriptor[j].length;
		 					  attribute.type = recordDescriptor[j].type;
		 					  attrs.push_back(attribute);
		 				  }
		 			  }

	}

}

Project:: ~Project()
{
	 itr = NULL;
     rm = NULL;
			//string tableName;
	  recordDescriptor.clear();
	  projectAttribute.clear();

}

NLJoin::NLJoin(Iterator *leftIn,                             // Iterator of input R
       TableScan *rightIn,                           // TableScan Iterator of input S
       const Condition &condition,                   // Join condition
       const unsigned numPages                       // Number of pages can be used to do join (decided by the optimizer)
)
{
	itrOut = NULL;
	itrIn = NULL;
	outBuf = NULL;
	itrOut = leftIn;
	itrIn = rightIn;
	cond.bRhsIsAttr = condition.bRhsIsAttr;
    cond.lhsAttr = condition.lhsAttr;
    cond.op = condition.op;
	cond.rhsValue.data = NULL;
    cond.rhsValue.data = condition.rhsValue.data;
		    //Ç³¿½±´
	cond.rhsValue.type = condition.rhsValue.type;
	cond.rhsAttr = condition.rhsAttr;
}
RC NLJoin::getNextTuple(void *data)
{
	vector<Attribute> attrOut;
	vector<Attribute> attrIn;
	itrOut->getAttributes(attrOut);
	itrIn->getAttributes(attrIn);
	void *inBuf = malloc(PAGE_SIZE);
	void *condOut = malloc(PAGE_SIZE);
	void *condIn = malloc(PAGE_SIZE);
	if(outBuf == NULL)
	{
		outBuf = malloc(PAGE_SIZE);
		if(itrOut->getNextTuple(outBuf) == -1)
		{
			free(inBuf);
			free(outBuf);
			free(condOut);
			free(condIn);
			cout<<"error1"<<endl;
			return -1;

		}
	}
	int len;
	int offset;
	int offset1;
	int fieldNum;
	AttrType leftType;

	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	while(1)
	{
		if(itrIn->getNextTuple(inBuf) == -1)
		{
			while(1)
			{
				if(itrOut->getNextTuple(outBuf) != -1)
				{
					itrIn->setIterator();
					if(itrIn->getNextTuple(inBuf) != -1)
					{
						break;
					}
				}
				else
				{
					free(inBuf);
					free(outBuf);
					free(condOut);
					free(condIn);
					cout<<"error2"<<endl;
					return -1;
				}
			}

		}
		offset = 0;
		len = 0;
		fieldNum = attrOut.size();
		for(int i = 0; i < fieldNum; i++)
		{
						switch(attrOut[i].type)
						{
						case TypeInt:
							len = sizeof(int);
							break;
						case TypeVarChar:
							len = *((int*) ((char*)outBuf + offset)) + sizeof(int);
							break;
						case TypeReal:
							len = sizeof(float);
							break;
						}
						if(attrOut[i].name == cond.lhsAttr)
						{
							memcpy(condOut, (char*)outBuf + offset, len);
							leftType = attrOut[i].type;
						}
						offset += len;
		}
		//cout<<"tuple left len is"<<offset<<endl;
		offset1 = 0;
		len = 0;
		fieldNum = attrIn.size();
		for(int i = 0; i < fieldNum; i++)
		{
								switch(attrIn[i].type)
								{
								case TypeInt:
									len = sizeof(int);
									break;
								case TypeVarChar:
									len = *((int*) ((char*)inBuf + offset1)) + sizeof(int);
									break;
								case TypeReal:
									len = sizeof(float);
									break;
								}
								if(attrIn[i].name == cond.rhsAttr)
								{
									memcpy(condIn, (char*)inBuf + offset1, len);
								}
								offset1 += len;
		}
		//cout<<"tuple right len is"<<offset1<<endl;
		bool judge = rbfm->judgeAttribute(condIn,condOut,cond.op,leftType);
		if(judge == true)
		{
			memcpy(data, outBuf, offset);
			memcpy((char*)data + offset, inBuf,offset1);
			free(inBuf);
			free(condOut);
			free(condIn);
			return 0;
		}
	}
}
void NLJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> outAttr;
	vector<Attribute> inAttr;
	itrOut->getAttributes(outAttr);
	itrIn->getAttributes(inAttr);
	attrs.clear();
	for(int i = 0; i < outAttr.size(); i++)
	{
		attrs.push_back(outAttr[i]);

	}
	for(int i = 0; i< inAttr.size(); i++)
	{
		attrs.push_back(inAttr[i]);
	}

}
NLJoin::~NLJoin()
{
    itrOut = NULL;
    itrIn = NULL;
    outBuf = NULL;
    cond.rhsValue.data = NULL;
}
INLJoin::INLJoin(Iterator *leftIn,                               // Iterator of input R
               IndexScan *rightIn,                             // IndexScan Iterator of input S
               const Condition &condition,                     // Join condition
               const unsigned numPages                         // Number of pages can be used to do join (decided by the optimizer)
       )
{
	itrOut = NULL;
	itrIn = NULL;
	outBuf = NULL;
	//attributeName.clear();
	itrOut = leftIn;
	itrIn = rightIn;
	cond.bRhsIsAttr = condition.bRhsIsAttr;
	cond.lhsAttr = condition.lhsAttr;
	cond.op = condition.op;
	cond.rhsValue.data = NULL;
	cond.rhsValue.data = condition.rhsValue.data;
	//Ç³¿½±´
	cond.rhsValue.type = condition.rhsValue.type;
	cond.rhsAttr = condition.rhsAttr;
}

RC INLJoin::getNextTuple(void *data)
{
	    vector<Attribute> attrOut;
	    vector<Attribute> attrIn;
		itrOut->getAttributes(attrOut);
		itrIn->getAttributes(attrIn);
		void *inBuf = malloc(PAGE_SIZE);
		void *condOut = malloc(PAGE_SIZE);
		int fieldNum;
		int len;
		int offset;
		int offset1;
		void* lowKey;
		void* highKey;
		bool lowKeyInclusive;
		bool highKeyInclusive;
		if(outBuf == NULL || (itrIn->getNextTuple(inBuf) == -1))
		{
			    if(outBuf == NULL)
			    {
			    	outBuf = malloc(PAGE_SIZE);
			    }
			    while(1)
			    {
			    	if(itrOut->getNextTuple(outBuf) == -1)
			    	{
			    		free(inBuf);
			    		free(outBuf);
			    		free(condOut);
			    		return -1;
			    	}
			    	//else
			    	//{
			    	    offset = 0;
			    	    len = 0;
			    		fieldNum = attrOut.size();
			    		for(int i = 0; i < fieldNum; i++)
			    		{
			    			switch(attrOut[i].type)
			    			{
			    			case TypeInt:
			    				len = sizeof(int);
			    				break;
			    			case TypeVarChar:
			    				len = *((int*) ((char*)outBuf + offset)) + sizeof(int);
			    				break;
			    			case TypeReal:
			    				len = sizeof(float);
			    				break;
			    			}
			    			if(attrOut[i].name == cond.lhsAttr)
			    			{
			    				memcpy(condOut, (char*)outBuf + offset, len);
			    			}
			    			offset += len;
			    		}
			    		lowKey = NULL;
			    		highKey = NULL;
			    		lowKeyInclusive = false;
			    		highKeyInclusive = false;
			    		switch(cond.op)
			    		{
			    		case EQ_OP:
			    		lowKey = condOut;
						highKey = condOut;
						lowKeyInclusive = true;
						highKeyInclusive = true;
						break;
					    case LE_OP:
						lowKeyInclusive = true;
					    case LT_OP:
						lowKey = condOut;
						break;
					    case GE_OP:
						highKeyInclusive = true;
					    case GT_OP:
						highKey = condOut;
						break;
					    default:
						break;
					    }
			    		itrIn->setIterator(lowKey,highKey,lowKeyInclusive,highKeyInclusive);
			    		if(itrIn->getNextTuple(inBuf) != -1)
			    		{
			    			break;
			    		}
			    }
		}
		offset1 = 0;
		len = 0;
		fieldNum = attrIn.size();
		for(int i = 0; i < fieldNum; i++)
		{
								switch(attrIn[i].type)
								{
								case TypeInt:
									len = sizeof(int);
									break;
								case TypeVarChar:
									len = *((int*) ((char*)inBuf + offset1)) + sizeof(int);
									break;
								case TypeReal:
									len = sizeof(float);
									break;
								}
								offset1 += len;
		}
		memcpy(data, outBuf, offset);
		memcpy((char*)data + offset, inBuf, offset1);
		free(inBuf);
		free(condOut);
		return 0;

}

INLJoin::~INLJoin()
{
	itrIn = NULL;
	itrOut = NULL;
	outBuf = NULL;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{
	vector<Attribute> outAttr;
	vector<Attribute> inAttr;
	itrOut->getAttributes(outAttr);
	itrIn->getAttributes(inAttr);
	attrs.clear();
	for(int i = 0; i < outAttr.size(); i++)
	{
		attrs.push_back(outAttr[i]);

	}
	for(int i = 0; i< inAttr.size(); i++)
	{
		attrs.push_back(inAttr[i]);
	}

}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr,AggregateOp op)
{
	itr = NULL;
	itr = input;
	this->aggAttr.length = aggAttr.length;
	this->aggAttr.name = aggAttr.name;
	this->aggAttr.type = aggAttr.type;
	this->op = op;
	gAttr.type = TypeVarChar;
	gAttr.length = 0;
	void * data1 = malloc(PAGE_SIZE);
	void * data = malloc(PAGE_SIZE);
	int tempInt;
	float tempFl;
	string attrName;
	attrName = aggAttr.name;
	strIntMap.clear();
	strFlMap.clear();
	vector<Attribute> recordDescriptor;
	input->getAttributes(recordDescriptor);
	while(input->getNextTuple(data1) != -1)
	{
		                        int offset = 0;
					    	    int len = 0;
					    	    int fieldNum = recordDescriptor.size();
					    		for(int i = 0; i < fieldNum; i++)
					    		{
					    			switch(recordDescriptor[i].type)
					    			{
					    			case TypeInt:
					    				len = sizeof(int);
					    				break;
					    			case TypeVarChar:
					    				len = *((int*) ((char*)data + offset)) + sizeof(int);
					    				break;
					    			case TypeReal:
					    				len = sizeof(float);
					    				break;
					    			}
					    			if(recordDescriptor[i].name == aggAttr.name)
					    			{
					    				memcpy(data, (char*)data1 + offset, len);
					    			}
					    			offset += len;
					    		}

		switch(aggAttr.type)
		{
		case TypeInt:
			tempInt= *((int*) data);
			if(strIntMap.find(attrName) != strIntMap.end())
			{
				strIntMap[attrName].count++;
				if(tempInt > strIntMap[attrName].max)
				{
					strIntMap[attrName].max = tempInt;
				}
				if(tempInt < strIntMap[attrName].min)
				{
					strIntMap[attrName].min = tempInt;
				}
				strIntMap[attrName].sum += tempInt;
			}
			else
			{
				strIntMap[attrName].count = 1;
				strIntMap[attrName].max = tempInt;
				strIntMap[attrName].min = tempInt;
				strIntMap[attrName].sum = tempInt;
			}
			break;
		case TypeReal:
			tempFl = *((float*) data);
			if(strFlMap.find(attrName) != strFlMap.end())
			{
							strFlMap[attrName].count++;
							if(tempFl > strFlMap[attrName].max)
							{
								strFlMap[attrName].max = tempFl;
							}
							if(tempInt < strFlMap[attrName].min)
							{
								strFlMap[attrName].min = tempFl;
							}
							strFlMap[attrName].sum += tempFl;
			}
			else
			{
							strFlMap[attrName].count = 1;
							strFlMap[attrName].max = tempFl;
							strFlMap[attrName].min = tempFl;
							strFlMap[attrName].sum = tempFl;
			}
			break;
		default:
			break;
		}
	}
	float sum;
	int count;
	switch(aggAttr.type)
	{
	case TypeInt:
		sum = (float)strIntMap[attrName].sum;
		count = strIntMap[attrName].count;
		strIntMap[attrName].avg = sum/count;
		itrStrInt = strIntMap.begin();
		break;
	case TypeReal:
		sum = (float)strFlMap[attrName].sum;
		count = strFlMap[attrName].count;
		strFlMap[attrName].avg = sum/count;
		itrStrFl = strFlMap.begin();
		break;
	}
	free(data);
	free(data1);
}

Aggregate::Aggregate(Iterator *input,                              // Iterator of input R
          Attribute aggAttr,                            // The attribute over which we are computing an aggregate
          Attribute gAttr,                              // The attribute over which we are grouping the tuples
          AggregateOp op)
{
	itr = input;
	this->aggAttr.length = aggAttr.length;
	this->aggAttr.name = aggAttr.name;
	this->aggAttr.type = aggAttr.type;
	this->gAttr.length = gAttr.length;
	this->gAttr.name = gAttr.name;
	this->gAttr.type = gAttr.type;
	cout<<"the group attribute is "<<gAttr.name<<endl;
	cout<<"the aggregate attribute is"<<aggAttr.name<<endl;
	this->op = op;
	int keyInt;
	float keyFl;
	string keyStr;
	int valueInt;
	float valueFl;
	strIntMap.clear();
	strFlMap.clear();
	intIntMap.clear();
	intFlMap.clear();
	flIntMap.clear();
	flFlMap.clear();
	void* data = malloc(PAGE_SIZE);
	vector<Attribute> recordDescriptor;
	input->getAttributes(recordDescriptor);
	int fieldNum;
	int offset;
	int len;
	AttrType keyType;
	AttrType valueType;
	keyType = gAttr.type;
	valueType = aggAttr.type;
	int count = 0;


	while(input->getNextTuple(data) != -1)
	{
		count++;

		fieldNum = recordDescriptor.size();
		offset = 0;
		for(int i = 0; i < fieldNum; i++)
		{
			                       //cout<<"the"<<i<<" the attribute name is "<<recordDescriptor[i].name<<endl;
					    			switch(recordDescriptor[i].type)
					    			{
					    			case TypeInt:
					    				len = sizeof(int);
					    				break;
					    			case TypeVarChar:
					    				len = *((int*) ((char*)data + offset)) + sizeof(int);
					    				break;
					    			case TypeReal:
					    				len = sizeof(float);
					    				break;
					    			}
					    			//cout<<"the name of the group attribute is "<<gAttr.name<<endl;
					    			if(recordDescriptor[i].name == gAttr.name)
					    				cout<<"the offset of group attribute is"<<offset<<endl;	{

					    				cout<<"key is the "<<i<<" th attribute in the tuple"<<endl;
					    				cout<<"the offset of group attribute is"<<offset<<endl;
					    				//cout<<"find the key"<<endl;
					    				switch(gAttr.type)
					    				{

					    				case TypeInt:
					    					keyInt = *((int*)(char*)data + offset);
					    					cout<<"keyInt is "<<keyInt<<endl;

					    					break;
					    				case TypeReal:
					    					keyFl = *((float*)(char*)data + offset);
					    					break;
					    				case TypeVarChar:
					    					keyStr = string((char*)data + offset, len);
					    					break;
					    				}
					    				break;
					    			}
					    			offset += len;
		}
		fieldNum = recordDescriptor.size();
		offset = 0;
		for(int i = 0; i < fieldNum; i++)
		{
							    			switch(recordDescriptor[i].type)
							    			{
							    			case TypeInt:
							    				len = sizeof(int);
							    				break;
							    			case TypeVarChar:
							    				len = *((int*) ((char*)data + offset)) + sizeof(int);
							    				break;
							    			case TypeReal:
							    				len = sizeof(float);
							    				break;
							    			}
							    			if(recordDescriptor[i].name == aggAttr.name)
							    			{
							    				switch(recordDescriptor[i].type)
							    				{
							    				case TypeInt:
							    					valueInt = *((int*)(char*)data + offset);
							    					break;
							    				case TypeReal:
							    					valueFl = *((float*)(char*)data + offset);
							    					break;
							    				default:
							    					break;
							    				}
							    				break;
							    			}
							    			offset += len;
		}
		switch(keyType)
		{
		case TypeVarChar:
			if(valueType == TypeInt)
			{
			            if(strIntMap.find(keyStr) != strIntMap.end())
			            {
							strIntMap[keyStr].count++;
							if(valueInt > strIntMap[keyStr].max)
							{
								strIntMap[keyStr].max = valueInt;
							}
							if(valueInt < strIntMap[keyStr].min)
							{
								strIntMap[keyStr].min = valueInt;
							}
							strIntMap[keyStr].sum += valueInt;
						}
						else
						{
							strIntMap[keyStr].count = 1;
							strIntMap[keyStr].max = valueInt;
							strIntMap[keyStr].min = valueInt;
							strIntMap[keyStr].sum = valueInt;
			            }
			}
			if(valueType == TypeReal)
			{
			            if(strFlMap.find(keyStr) != strFlMap.end())
			            {
							strFlMap[keyStr].count++;
							if(valueFl > strFlMap[keyStr].max)
							{
								strFlMap[keyStr].max = valueFl;
							}
							if(valueFl < strFlMap[keyStr].min)
							{
								strFlMap[keyStr].min = valueFl;
							}
							strFlMap[keyStr].sum += valueFl;
						}
						else
						{
							strFlMap[keyStr].count = 1;
							strFlMap[keyStr].max = valueFl;
							strFlMap[keyStr].min = valueFl;
							strFlMap[keyStr].sum = valueFl;
			            }
			}
			break;
		case TypeInt:
			if(valueType == TypeInt)
			{
				//cout<<"the key is "<<keyInt<<endl;
				//cout<<"the value is "<<valueInt<<endl;
			            if(intIntMap.find(keyInt) != intIntMap.end())
			            {
							intIntMap[keyInt].count++;
							if(valueInt > intIntMap[keyInt].max)
							{
								intIntMap[keyInt].max = valueInt;
							}
							if(valueInt < intIntMap[keyInt].min)
							{
								intIntMap[keyInt].min = valueInt;
							}
							intIntMap[keyInt].sum += valueInt;
						}
						else
						{
							cout<<"one more key is found"<<endl;
							intIntMap[keyInt].count = 1;
							intIntMap[keyInt].max = valueInt;
							intIntMap[keyInt].min = valueInt;
							intIntMap[keyInt].sum = valueInt;
			            }
			}
			if(valueType == TypeReal)
			{
			            if(intFlMap.find(keyInt) != intFlMap.end())
			            {
							intFlMap[keyInt].count++;
							if(valueFl > intFlMap[keyInt].max)
							{
								intFlMap[keyInt].max = valueFl;
							}
							if(valueFl < intFlMap[keyInt].min)
							{
								intFlMap[keyInt].min = valueFl;
							}
							intFlMap[keyInt].sum += valueFl;
						}
						else
						{
							intFlMap[keyInt].count = 1;
							intFlMap[keyInt].max = valueFl;
							intFlMap[keyInt].min = valueFl;
							intFlMap[keyInt].sum = valueFl;
			            }
			}
			break;
		case TypeReal:
			if(valueType == TypeInt)
			{
					            if(flIntMap.find(keyFl) != flIntMap.end())
					            {
									flIntMap[keyFl].count++;
									if(valueInt > flIntMap[keyFl].max)
									{
										flIntMap[keyFl].max = valueInt;
									}
									if(valueInt < flIntMap[keyFl].min)
									{
										flIntMap[keyFl].min = valueInt;
									}
									flIntMap[keyFl].sum += valueInt;
								}
								else
								{
									flIntMap[keyFl].count = 1;
									flIntMap[keyFl].max = valueInt;
									flIntMap[keyFl].min = valueInt;
									flIntMap[keyFl].sum = valueInt;
					            }
			}
			if(valueType == TypeReal)
			{
					            if(flFlMap.find(keyFl) != flFlMap.end())
					            {
					            	flFlMap[keyFl].count++;
									if(valueFl > flFlMap[keyFl].max)
									{
										flFlMap[keyFl].max = valueFl;
									}
									if(valueFl < flFlMap[keyFl].min)
									{
										flFlMap[keyFl].min = valueFl;
									}
									flFlMap[keyFl].sum += valueFl;
								}
								else
								{
									flFlMap[keyFl].count = 1;
									flFlMap[keyFl].max = valueFl;
									flFlMap[keyFl].min = valueFl;
									flFlMap[keyFl].sum = valueFl;
					            }
			}
			break;
		 }
	}
	switch(keyType)
	{
	case TypeVarChar:
		if(valueType == TypeInt)
		{
			itrStrInt = strIntMap.begin();
		}
		if(valueType == TypeReal)
		{
			itrStrFl = strFlMap.begin();
		}
		break;
	case TypeInt:
		if(valueType == TypeInt)
		{
			itrIntInt = intIntMap.begin();

		}
		if(valueType == TypeReal)
		{
			itrIntFl = intFlMap.begin();
		}
		break;
	case TypeReal:
		if(valueType == TypeInt)
		{
			itrFlInt = flIntMap.begin();
		}
		if(valueType == TypeReal)
		{
			itrFlFl = flFlMap.begin();
		}
		break;
	}
	cout<<"scan "<<count<<" number of tuples"<<endl;
}
//MIN = 0, MAX, SUM, AVG, COUNT
RC Aggregate::getNextTuple(void *data)
{
	int tempInt;
	float tempFloat;
	float sum;
	int len;
	string tempStr;
	int offset;
	switch(gAttr.type)
	{
	case TypeVarChar:
		if(aggAttr.type == TypeInt)
		{
			if(itrStrInt != strIntMap.end())
			{
				offset = 0;
				if(gAttr.length != 0)
				{
				tempStr = itrStrInt->first;
				len = itrStrInt->first.size();
				memcpy(data,&len, sizeof(int));
				offset += sizeof(int);
				memcpy((char*)data + offset, tempStr.c_str(),len);
				offset += len;
				}

				switch(op)
				{
				case MIN:
					tempInt = itrStrInt->second.min;
					memcpy((char*)data + offset,&tempInt,sizeof(int));
					break;
				case MAX:
					tempInt = itrStrInt->second.max;
					memcpy((char*)data + offset,&tempInt,sizeof(int));
					break;
				case SUM:
					tempInt = itrStrInt->second.sum;
					memcpy((char*)data + offset,&tempInt,sizeof(int));
					break;
				case AVG:
					sum = (float)itrStrInt->second.sum;
					tempInt = itrStrInt->second.count;
					tempFloat = sum/tempInt;
					memcpy((char*)data + offset,&tempFloat,sizeof(float));
					break;
				case COUNT:
					tempInt = itrStrInt->second.count;
					memcpy((char*)data + offset,&tempInt,sizeof(int));
					break;
				}
				itrStrInt++;
			}
			else
			{
				return -1;
			}
		}
		if(aggAttr.type == TypeReal)
		{
					if(itrStrFl != strFlMap.end())
					{
						offset = 0;
						if(gAttr.length != 0)
						{
						tempStr = itrStrFl->first;
						len = itrStrFl->first.size();
						memcpy(data,&len, sizeof(int));
						offset += sizeof(int);
						memcpy((char*)data + offset, tempStr.c_str(),len);
						offset += len;
						}

						switch(op)
						{
						case MIN:
							tempFloat = itrStrFl->second.min;
							memcpy((char*)data + offset,&tempFloat,sizeof(float));
							break;
						case MAX:
							tempFloat = itrStrFl->second.max;
							memcpy((char*)data + offset,&tempFloat,sizeof(float));
							break;
						case SUM:
							tempFloat = itrStrFl->second.sum;
							memcpy((char*)data + offset,&tempFloat,sizeof(float));
							break;
						case AVG:
							sum = (float)itrStrFl->second.sum;
							tempInt = itrStrInt->second.count;
							tempFloat = sum/tempInt;
							memcpy((char*)data + offset,&tempFloat,sizeof(float));
							break;
						case COUNT:
							tempInt = itrStrFl->second.count;
							memcpy((char*)data + offset,&tempInt,sizeof(int));
							break;
						}
						itrStrFl++;
					}
					else
					{
						return -1;
					}
		}
		break;

	case TypeInt:
		if(aggAttr.type == TypeInt)
		{
					if(itrIntInt != intIntMap.end())
					{
						offset = 0;
						if(gAttr.length != 0)
						{
						tempInt = itrIntInt->first;
						memcpy(data,&tempInt, sizeof(int));
						offset += sizeof(int);
						}
						switch(op)
						{
						case MIN:
							tempInt = itrIntInt->second.min;
							memcpy((char*)data + offset,&tempInt,sizeof(int));
							break;
						case MAX:
							tempInt = itrIntInt->second.max;
							memcpy((char*)data + offset,&tempInt,sizeof(int));
							break;
						case SUM:
							tempInt = itrIntInt->second.sum;
							memcpy((char*)data + offset,&tempInt,sizeof(int));
							break;
						case AVG:
							sum = (float)itrIntInt->second.sum;
							tempInt = itrIntInt->second.count;
							tempFloat = sum/tempInt;
							memcpy((char*)data + offset,&tempFloat,sizeof(float));
							break;
						case COUNT:
							tempInt = itrIntInt->second.count;
							memcpy((char*)data + offset,&tempInt,sizeof(int));
							break;
						}
						itrIntInt++;
					}
					else
					{
						return -1;
					}
		}
		if(aggAttr.type == TypeReal)
		{
							if(itrIntFl != intFlMap.end())
							{
								offset = 0;
								if(gAttr.length != 0)
								{
								tempInt = itrIntInt->first;
								memcpy(data,&tempInt, sizeof(int));
								offset += sizeof(int);
								}

								switch(op)
								{
								case MIN:
									tempFloat = itrIntFl->second.min;
									memcpy((char*)data + offset,&tempFloat,sizeof(float));
									break;
								case MAX:
									tempFloat = itrIntFl->second.max;
									memcpy((char*)data + offset,&tempFloat,sizeof(float));
									break;
								case SUM:
									tempFloat = itrIntFl->second.sum;
									memcpy((char*)data + offset,&tempFloat,sizeof(float));
									break;
								case AVG:
									sum = (float)itrIntFl->second.sum;
									tempInt = itrIntFl->second.count;
									tempFloat = sum/tempInt;
									memcpy((char*)data + offset,&tempFloat,sizeof(float));
									break;
								case COUNT:
									tempInt = itrIntFl->second.count;
									memcpy((char*)data + offset,&tempInt,sizeof(int));
									break;
								}
								itrIntFl++;
							}
							else
							{
								return -1;
							}
		}
		break;
	case TypeReal:
		if(aggAttr.type == TypeInt)
		{
							if(itrFlInt != flIntMap.end())
							{
								offset = 0;
								if(gAttr.length != 0)
								{
								tempFloat = itrFlInt->first;
								memcpy(data,&tempFloat, sizeof(float));
								offset += sizeof(float);
								}
								switch(op)
								{
								case MIN:
									tempInt = itrFlInt->second.min;
									memcpy((char*)data + offset,&tempInt,sizeof(int));
									break;
								case MAX:
									tempInt = itrFlInt->second.max;
									memcpy((char*)data + offset,&tempInt,sizeof(int));
									break;
								case SUM:
									tempInt = itrFlInt->second.sum;
									memcpy((char*)data + offset,&tempInt,sizeof(int));
									break;
								case AVG:
									sum = (float)itrFlInt->second.sum;
									tempInt = itrFlInt->second.count;
									tempFloat = sum/tempInt;
									memcpy((char*)data + offset,&tempFloat,sizeof(float));
									break;
								case COUNT:
									tempInt = itrFlInt->second.count;
									memcpy((char*)data + offset,&tempInt,sizeof(int));
									break;
								}
								itrFlInt++;
							}
							else
							{
								return -1;
							}
		}
		if(aggAttr.type == TypeReal)
		{
									if(itrFlFl != flFlMap.end())
									{
										offset = 0;
										if(gAttr.length != 0)
										{
										tempFloat = itrFlFl->first;
										memcpy(data,&tempFloat, sizeof(float));
										offset += sizeof(float);
										}

										switch(op)
										{
										case MIN:
											tempFloat = itrFlFl->second.min;
											memcpy((char*)data + offset,&tempFloat,sizeof(float));
											break;
										case MAX:
											tempFloat = itrFlFl->second.max;
											memcpy((char*)data + offset,&tempFloat,sizeof(float));
											break;
										case SUM:
											tempFloat = itrFlFl->second.sum;
											memcpy((char*)data + offset,&tempFloat,sizeof(float));
											break;
										case AVG:
											sum = (float)itrFlFl->second.sum;
											tempInt = itrFlFl->second.count;
											tempFloat = sum/tempInt;
											memcpy((char*)data + offset,&tempFloat,sizeof(float));
											break;
										case COUNT:
											tempInt = itrFlFl->second.count;
											memcpy((char*)data + offset,&tempInt,sizeof(int));
											break;
										}
										itrFlFl++;
									}
									else
									{
										return -1;
									}
		}
		break;
	}
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	int len;
	for(int i = 0; i < aggAttr.name.size(); i++)
	{
		if(aggAttr.name[i] == '.')
		{
			len = i;
			break;
		}
	}
	string tableName = aggAttr.name.substr(0,len);
	string attrName = aggAttr.name.substr(len + 1, aggAttr.name.size() - len -1);
	string opName;
	Attribute attrTemp;
	switch(op)
	{
	case MAX:
		opName = "MAX";
		attrTemp.type = aggAttr.type;
		break;
	case MIN:
		opName = "MIN";
		attrTemp.type = aggAttr.type;
		break;
	case SUM:
		opName = "SUM";
		attrTemp.type = aggAttr.type;
		break;
	case COUNT:
		opName = "COUNT";
		attrTemp.type = TypeInt;
		break;
	case AVG:
		opName = "AVG";
		attrTemp.type = TypeReal;
		break;
	}
	attrTemp.name = opName+"("+tableName+"."+attrName+")";
	attrs.push_back(attrTemp);
}
Aggregate::~Aggregate()
{
	itr = NULL;
	strIntMap.clear();
	strFlMap.clear();
	intIntMap.clear();
	intFlMap.clear();
	flIntMap.clear();
	flFlMap.clear();

}
