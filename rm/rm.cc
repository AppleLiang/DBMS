
#include "rm.h"
#include <stdio.h>
#include <string.h>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	im = IndexManager::instance();
	if(!rbfm->fileExists("table.tbl"))
	{
		rbfm->createFile("table.tbl");
		FileHandle tableFileHandle;
		rbfm->openFile("table.tbl", tableFileHandle);
		void *data = malloc(PAGE_SIZE);
		RID rid;
		vector<Attribute> rd;
		createTablerd(rd);
		prepareTableTuple("table", 0, 3, data);
		rbfm->insertRecord(tableFileHandle, rd, data, rid);
		//cout<<rid.pageNum<<endl;
		//cout<<rid.slotNum<<endl;
		prepareTableTuple("Column", 0, 5, data);
		rbfm->insertRecord(tableFileHandle, rd, data, rid);
		//cout<<rid.pageNum<<endl;
		//cout<<rid.slotNum<<endl;
		rbfm->closeFile(tableFileHandle);
		rbfm->createFile("column.tbl");
		FileHandle columnFileHandle;
		rbfm->openFile("column.tbl", columnFileHandle);
		createColumnrd(rd);
		prepareColumnTuple("table","tableName",(int)TypeVarChar,0,256,data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("table","tableType",(int)TypeVarChar,1,256,data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("table","numCol",(int)TypeInt,2,sizeof(int),data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("column","tableName",(int)TypeVarChar,0,256,data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("column","colName",(int)TypeVarChar,1,256,data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("column","colType",(int)TypeInt,2,sizeof(int),data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("column","colPosition",(int)TypeInt,3,sizeof(int),data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		prepareColumnTuple("column","colLength",(int)TypeInt,4,sizeof(int),data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
		rbfm->closeFile(columnFileHandle);
	}
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	if(rbfm->fileExists(tableName))
	{
		return -1;
	}
	rbfm->createFile(tableName);
	void* data = malloc(PAGE_SIZE);
	RID rid;
	vector<Attribute> rd;
	RC rc;

	FileHandle tableFileHandle;
	rbfm->openFile("table.tbl",tableFileHandle);
	createTablerd(rd);
	int attrSize = (int)attrs.size();
	prepareTableTuple(tableName, 1, attrSize, data);
	rbfm->insertRecord(tableFileHandle, rd, data, rid);
	rbfm->closeFile(tableFileHandle);


	FileHandle columnFileHandle;
	rbfm->openFile("column.tbl",columnFileHandle);
	createColumnrd(rd);
	for(int i = 0; i < attrs.size(); i++)
	{
		prepareColumnTuple(tableName, attrs[i].name, (int)attrs[i].type, i, attrs[i].length, data);
		rbfm->insertRecord(columnFileHandle, rd, data, rid);
	}
	rbfm->closeFile(columnFileHandle);


	free(data);
	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	RID id;
	RM_ScanIterator rmsi;
	void* value = malloc(tableName.size() + sizeof(int));
	void* data = malloc(PAGE_SIZE);
	const char* str = tableName.c_str();
	int len = (int)tableName.size();
	memcpy(value,&len,sizeof(int));
	memcpy((char*)value + sizeof(int), str, len);
	vector<string> attributeNames;
	attributeNames.clear();
	string table = "table.tbl";
	string conditionAttribute = "tableName";
	scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
	if(rmsi.getNextTuple(id,data) == 0)
	{
		readAttribute(table, id, "tableType", data);
		int tabletype = *((int*)data);
		if( tabletype == 0)
		{
			free(value);
			free(data);
			rmsi.close();
			return -1;
		}
		else
		{
			deleteTuple(table,id);
			rmsi.close();
			vector<Attribute> recordDescriptor;
			getAttributes(tableName,recordDescriptor);
			for(int i = 0; i < recordDescriptor.size(); i++)
			{
				string attributeName = recordDescriptor[i].name;
				string indexTable = tableName + "." + attributeName;
				if(rbfm->fileExists(indexTable))
				{
					RC rc = destroyIndex(tableName,attributeName);
					if(rc != 0)
					{
						free(value);
						free(data);
						return -1;


					}
				}

			}
			table = "column.tbl";
			string conditionAttribute = "tableName";
			scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
			while(rmsi.getNextTuple(id,data) == 0)
			{
				deleteTuple(table,id);
			}
			rmsi.close();
			rbfm->destroyFile(tableName);
			free(value);
			free(data);
			return 0;
		}

	}
	rmsi.close();
	free(value);
	free(data);
	return -1;
}
RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	string indexTable = tableName + "." + attributeName;
	if(!(rbfm->fileExists(indexTable)))
	{
			return 0;
	}
	return im->destroyFile(indexTable);

}
RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	string indexTable = tableName + "." + attributeName;
	if(rbfm->fileExists(indexTable))
	{
		return -1;
	}
	im->createFile(indexTable);
	//此处的createFile可能需要修改
	RM_ScanIterator rmsi;
	void* data = malloc(PAGE_SIZE);
	RID rid;
	vector<string> attributeNames;
	attributeNames.push_back(attributeName);
	scan(tableName,attributeName,NO_OP,NULL,attributeNames,rmsi);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	int fieldNum = recordDescriptor.size();
	Attribute indexAttr;
	for(int i = 0; i < fieldNum; i++)
	{
		if(recordDescriptor[i].name == attributeName)
		{
			indexAttr.name = recordDescriptor[i].name;
			indexAttr.length = recordDescriptor[i].length;
			indexAttr.type = recordDescriptor[i].type;
			break;
		}
	}
	FileHandle fileHandle;
	rbfm->openFile(indexTable,fileHandle);
	while(rmsi.getNextTuple(rid,data) != -1)
	{
		rc = im->insertEntry(fileHandle, indexAttr, data, rid);
		if(rc == -1)
		{
			rbfm->closeFile(fileHandle);
			free(data);
			rmsi.close();
			return -1;
		}
	}
	rbfm->closeFile(fileHandle);
	free(data);
	rmsi.close();
	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	if(tableName == "table.tbl")
	{
		createTablerd(attrs);
		return 0;
	}
	if(tableName == "column.tbl")
	{
		createColumnrd(attrs);
		return 0;
	}

	RID id;
	RM_ScanIterator rmsi;
	void* value = malloc(tableName.size() + sizeof(int));
	void* data = malloc(PAGE_SIZE);
	const char* str = tableName.c_str();
	int len = (int)tableName.size();
	memcpy(value,&len,sizeof(int));
	memcpy((char*)value + sizeof(int), str, len);

	vector<string> attributeNames;
	attributeNames.push_back("colName");
	attributeNames.push_back("colType");
	attributeNames.push_back("colPosition");
	attributeNames.push_back("colLength");
	string table = "column.tbl";
	string conditionAttribute = "tableName";
	scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
	//cout<<"scan end"<<endl;
	vector<Attribute>columnrd;
	vector<Attribute1> columntemp;
	createColumnrd(columnrd);
	while(rmsi.getNextTuple(id,data) == 0)
	{
		  int offset = 0;
		  int strLength;
		  Attribute1 attr;
		 /* memcpy(&strLength, (char *)data + offset,  sizeof(int));
		  offset += sizeof(int);
		  attr.tname = string((char*)data + offset, strLength);
		  cout<<attr.tname<<endl;
		  offset += strLength;*/

		  memcpy(&strLength, (char *)data + offset,  sizeof(int));
		  offset += sizeof(int);
		  attr.name = string((char*)data + offset, strLength);
		  offset += strLength;

		  int colType;
		  memcpy(&colType, (char *)data + offset, sizeof(int));
		  attr.type = (AttrType)colType;
		  offset += sizeof(int);

		  int position;
		  memcpy(&position, (char *)data + offset, sizeof(int));
		  attr.colPosition = position;
		  offset += sizeof(int);

		  int length;
		  memcpy(&length,(char *)data + offset, sizeof(int));
		  attr.length = length;
		  offset += sizeof(int);

		  columntemp.push_back(attr);
	}
	for(int i = 0; i < columntemp.size(); i++)
	{
		for(int j = 0; j < columntemp.size(); j++)
		{
			if(columntemp[j].colPosition == i)
			{
				Attribute attrTemp;
				attrTemp.length = columntemp[j].length;
				attrTemp.name = columntemp[j].name;
				attrTemp.type = columntemp[j].type;
				attrs.push_back(attrTemp);
			}
		}
	}
	rmsi.close();
	free(data);
	free(value);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(!(rbfm->fileExists(tableName)))
	{
	return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->insertRecord(handle, recordDescriptor, data, rid);
	rbfm->closeFile(handle);
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
			string indexTable = tableName + "." + recordDescriptor[i].name;
			if(rbfm->fileExists(indexTable))
			{
				void *buffer = malloc(PAGE_SIZE);
				readAttribute(tableName, rid, recordDescriptor[i].name, buffer);
				FileHandle indexHandle;
				rbfm->openFile(indexTable,indexHandle);
				RC rc = im->insertEntry(indexHandle, recordDescriptor[i], buffer, rid);
				rbfm->closeFile(indexHandle);
				free(buffer);
				if(rc == -1)
				{
					return -1;
				}

			}
	}
	return rc;
}

RC RelationManager::printTuple(const string &tableName, void*data)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}

		vector<Attribute> recordDescriptor;
		getAttributes(tableName, recordDescriptor);
		RC rc = rbfm->printRecord(recordDescriptor, data);
		return rc;

}

RC RelationManager::deleteTuples(const string &tableName)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	RM_ScanIterator rmsi;
	void* value = malloc(tableName.size() + sizeof(int));
	void* data = malloc(PAGE_SIZE);
	const char* str = tableName.c_str();
	int len = (int)tableName.size();
	memcpy(value,&len,sizeof(int));
	memcpy((char*)value + sizeof(int), str, len);
	vector<string> attributeNames;
	attributeNames.clear();
	string table = "table.tbl";
	string conditionAttribute = "tableName";
	scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
	RID id;
	if(rmsi.getNextTuple(id,data) != 0)
	{
		free(value);
		free(data);
		rmsi.close();
		return -1;
	}
	readAttribute(table, id, "tableType", data);
	int tabletype = *((int*)data);
	if(tabletype == 0)
	{
		free(value);
		free(data);
		rmsi.close();
		return -1;
	}
	rmsi.close();
	free(value);
	free(data);

	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->deleteRecords(handle);
	rbfm->closeFile(handle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
						string indexTable = tableName + "." + recordDescriptor[i].name;
						if(rbfm->fileExists(indexTable))
						{
							im->destroyFile(indexTable);
							im->createFile(indexTable);
							if(rc == -1)
							{
								return -1;
							}

						}
	}
	return rc;

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	RM_ScanIterator rmsi;
	void* value = malloc(tableName.size() + sizeof(int));
	void* data = malloc(PAGE_SIZE);
	const char* str = tableName.c_str();
	int len = (int)tableName.size();
	memcpy(value,&len,sizeof(int));
	memcpy((char*)value + sizeof(int), str, len);
	vector<string> attributeNames;
	attributeNames.clear();
	string table = "table.tbl";
	string conditionAttribute = "tableName";
	scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
	RID id;
	if(rmsi.getNextTuple(id,data) != 0)
	{
		free(value);
		free(data);
		rmsi.close();
		return -1;
	}

	readAttribute(table, id, "tableType", data);
	int tabletype = *((int*)data);
	if(tabletype == 0)
	{
		free(value);
		free(data);
		rmsi.close();
		return -1;
	}
	rmsi.close();
	free(value);
	free(data);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
					string indexTable = tableName + "." + recordDescriptor[i].name;
					if(rbfm->fileExists(indexTable))
					{
						void *buffer = malloc(PAGE_SIZE);
						readAttribute(tableName, rid, recordDescriptor[i].name, buffer);
						FileHandle indexHandle;
						rbfm->openFile(indexTable,indexHandle);
						RC rc = im->deleteEntry(indexHandle, recordDescriptor[i], buffer, rid);
						rbfm->closeFile(indexHandle);
						free(buffer);
						if(rc == -1)
						{
							return -1;
						}

					}
	}
	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->deleteRecord(handle, recordDescriptor, rid);
	rbfm->closeFile(handle);
	return rc;

}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(!(rbfm->fileExists(tableName)))
	{
	return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	FileHandle handle;

	for(int i = 0; i < recordDescriptor.size(); i++)
	{
		string indexTable = tableName + "." + recordDescriptor[i].name;
		if(rbfm->fileExists(indexTable))
		{
			void *buffer = malloc(PAGE_SIZE);
			readAttribute(tableName, rid, recordDescriptor[i].name, buffer);
			FileHandle indexHandle;
		    rbfm->openFile(indexTable,indexHandle);
			RC rc = im->deleteEntry(indexHandle, recordDescriptor[i], buffer, rid);
			rbfm->closeFile(indexHandle);
			free(buffer);
			if(rc == -1)
			{
				return -1;
			}

		}

	}
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->updateRecord(handle, recordDescriptor, data, rid);
	rbfm->closeFile(handle);
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
			string indexTable = tableName + "." + recordDescriptor[i].name;
			if(rbfm->fileExists(indexTable))
			{
				void *buffer = malloc(PAGE_SIZE);
				readAttribute(tableName, rid, recordDescriptor[i].name, buffer);
				FileHandle indexHandle;
			    rbfm->openFile(indexTable,indexHandle);
				RC rc = im->insertEntry(indexHandle, recordDescriptor[i], buffer, rid);
				rbfm->closeFile(indexHandle);
				free(buffer);
				if(rc == -1)
				{
					return -1;
				}

			}

	}
	return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	if(!(rbfm->fileExists(tableName)))
	{
	return -1;
	}
	//cout<<"readTuple"<<endl;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->readRecord(handle, recordDescriptor, rid, data);
	rbfm->closeFile(handle);
	return rc;

}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	if(!(rbfm->fileExists(tableName)))
	{
	return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->readAttribute(handle, recordDescriptor, rid, attributeName, data);
	rbfm->closeFile(handle);

	return rc;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	if(!(rbfm->fileExists(tableName)))
	{
	return -1;
	}

	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	FileHandle handle;
	rbfm->openFile(tableName,handle);
	RC rc = rbfm->reorganizePage(handle, recordDescriptor, pageNumber);
	rbfm->closeFile(handle);
	return rc;
}

RC RelationManager::indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator)
{
	string indexTable = tableName + "." + attributeName;
	if(!(rbfm->fileExists(indexTable)))
	{
		cout<<"index file doesn't exist"<<endl;
		return -1;
	}
	FileHandle fileHandle;
	rbfm->openFile(indexTable, fileHandle);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	int fieldNum = recordDescriptor.size();
	Attribute indexAttr;
	for(int i = 0; i < fieldNum; i++)
	{
		if(recordDescriptor[i].name == attributeName)
		{
			indexAttr.name = recordDescriptor[i].name;
			indexAttr.length = recordDescriptor[i].length;
			indexAttr.type = recordDescriptor[i].type;
			break;
		}
	}
	RC rc = im->scan(fileHandle, indexAttr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
	rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::scan(const string &tableName, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RM_ScanIterator &rm_ScanIterator)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);
	rbfm->openFile(tableName, rm_ScanIterator.rbfm_ScanIterator.handle);
	return rbfm->scan(rm_ScanIterator.rbfm_ScanIterator.handle, attrs, conditionAttribute, compOp, value, attributeNames,rm_ScanIterator.rbfm_ScanIterator);

}

// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	    if(!(rbfm->fileExists(tableName)))
		{
			return -1;
		}
		RC rc;
		void* value = malloc(tableName.size() + sizeof(int));
		void* data = malloc(PAGE_SIZE);
		void* data1 = malloc(PAGE_SIZE);
		const char* str = tableName.c_str();
		int len = (int)tableName.size();
		memcpy(value,&len,sizeof(int));
		memcpy((char*)value + sizeof(int), str, len);
		vector<string> attributeNames;
		//update table table
		memset(data,0,PAGE_SIZE);
		RM_ScanIterator rmsi1;
		attributeNames.clear();
		attributeNames.push_back("colName");
		attributeNames.push_back("colType");
		attributeNames.push_back("colPosition");
		attributeNames.push_back("colLength");
		string conditionAttribute = "tableName";
		string table1 = "column.tbl";
		scan(table1,conditionAttribute,EQ_OP,value,attributeNames,rmsi1);
		RID id;
		while(rmsi1.getNextTuple(id,data) == 0)
		{
			 int offset = 0;
			 int strLength;
			 Attribute1 attr;
			 memcpy(&strLength, (char *)data + offset,  sizeof(int));
			 offset += sizeof(int);
			 attr.name = string((char*)data + offset, strLength);
			 offset += strLength;
			 int colType;
			 memcpy(&colType, (char *)data + offset, sizeof(int));
			 attr.type = (AttrType)colType;
			 offset += sizeof(int);
			 int position;
			 memcpy(&position, (char *)data + offset, sizeof(int));
			 attr.colPosition = position;
			 offset += sizeof(int);
			 int length;
			 memcpy(&length,(char *)data + offset, sizeof(int));
			 attr.length = length;
			 offset += sizeof(int);
			 if(attr.name == attributeName)
			 {
				memset(data1,0,PAGE_SIZE);
				prepareColumnTuple(tableName, attr.name, (int)attr.type, attr.colPosition, 0, data1);
				rc = updateTuple(table1,data1,id);
				free(data);
				free(value);
				free(data1);
				rmsi1.close();
				return rc;
			 }

		}
		free(data);
	    free(value);
		free(data1);
		rmsi1.close();
		return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	RC rc;
	RM_ScanIterator rmsi;
	void* value = malloc(tableName.size() + sizeof(int));
	void* data = malloc(PAGE_SIZE);
	void* data1 = malloc(PAGE_SIZE);
	const char* str = tableName.c_str();
	int len = (int)tableName.size();
	memcpy(value,&len,sizeof(int));
	memcpy((char*)value + sizeof(int), str, len);
	vector<string> attributeNames;
	attributeNames.clear();
	string table = "table.tbl";
	string conditionAttribute = "tableName";
	scan(table,conditionAttribute,EQ_OP,value,attributeNames,rmsi);
	RID id;
	if(rmsi.getNextTuple(id,data) != 0)
	{
		free(value);
		free(data);
		free(data1);
		rmsi.close();
		cout<<"search tablename in table table wrong"<<endl;
		return -1;
	}
	rc = readAttribute(table, id, "colNum", data);
	if(rc != 0)
	{
		free(value);
		free(data);
		free(data1);
		rmsi.close();
		return -1;
	}
	int colNum = *((int*)data) + 1;
	prepareTableTuple(tableName, 1, colNum, data1);
	rc = updateTuple(table, data1, id);
	if(rc != 0)
	{
		free(value);
		free(data);
		free(data1);
		rmsi.close();
		cout<<"update tuple wrong"<<endl;
		return -1;
	}
	rmsi.close();
	//update table table
	RID rid;
	vector<Attribute> recordDescriptor;
	FileHandle columnFileHandle;
	rc = rbfm->openFile("column.tbl",columnFileHandle);
	if(rc != 0)
	{
		free(value);
		free(data);
		free(data1);
		cout<<"open file wrong"<<endl;
		//rmsi.close();
		return -1;
	}
	createColumnrd(recordDescriptor);
	memset(data, 0, PAGE_SIZE);
	prepareColumnTuple(tableName, attr.name, (int)attr.type, colNum-1, attr.length, data);
	rc = rbfm->insertRecord(columnFileHandle, recordDescriptor, data, rid);
	if(rc != 0)
	{
		free(value);
		free(data);
		free(data1);
		//rmsi.close();
		cout<<"add attribute wrong"<<endl;
		return -1;
	}
	rc = rbfm->closeFile(columnFileHandle);
	free(value);
	free(data);
	free(data1);
	return rc;

}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	if(!(rbfm->fileExists(tableName)))
	{
		return -1;
	}
	RM_ScanIterator rmsi;
	string newTableName = tableName + "backup";
	vector<Attribute> tableAttrs;
	getAttributes(tableName, tableAttrs);
	createTable(newTableName, tableAttrs);
	RID rid;
	void* returnedData;
	vector<string> attrs;
	for(int i = 0; i < tableAttrs.size(); i++)
	{
		attrs.push_back(tableAttrs[i].name);
	}
	RID rid2;
	scan(tableName, "", NO_OP, NULL, attrs, rmsi);
	while(rmsi.getNextTuple(rid, returnedData) != RM_EOF)
	{
		insertTuple(newTableName, returnedData, rid2);
	}
	rmsi.close();
	deleteTable(tableName);
	createTable(tableName, tableAttrs);
	RM_ScanIterator rmsi2;
	scan(newTableName, "", NO_OP, NULL, attrs, rmsi2);
	while(rmsi2.getNextTuple(rid, returnedData) != RM_EOF)
	{
		insertTuple(tableName, returnedData, rid2);
	}
    return 0;
}

void RelationManager::prepareTableTuple(string tableName, int tableType, int colNum, void* buffer)
{
	  int offset = 0;
	  int strLength = (int)tableName.size();
	  memcpy((char *)buffer + offset, &strLength, sizeof(int));
	  offset += sizeof(int);
	  memcpy((char *)buffer + offset, tableName.c_str(), strLength);
	  offset += strLength;
	  memcpy((char *)buffer + offset, &tableType, sizeof(int));
	  offset += sizeof(int);
	  memcpy((char *)buffer + offset, &colNum, sizeof(int));
	  offset += sizeof(int);

}

void RelationManager::prepareColumnTuple(string tableName, string colName, int colType, int colPosition, int maxSize, void* buffer)
{
	  int offset = 0;
	  int strLength = (int)tableName.size();
	  memcpy((char *)buffer + offset, &strLength, sizeof(int));
	  offset += sizeof(int);
	  memcpy((char *)buffer + offset, tableName.c_str(), strLength);
	  offset += strLength;

	  strLength = (int)colName.size();
	  memcpy((char *)buffer + offset, &strLength, sizeof(int));
	  offset += sizeof(int);
	  memcpy((char *)buffer + offset, colName.c_str(), strLength);
	  offset += strLength;

	  //strLength = (int)colType.size();
	  memcpy((char *)buffer + offset, &colType, sizeof(int));
	  offset += sizeof(int);
	  //memcpy((char *)buffer + offset, colType.c_str(), strLength);
	  //offset += strLength;

	  memcpy((char *)buffer + offset, &colPosition, sizeof(int));
	  offset += sizeof(int);
	  memcpy((char *)buffer + offset, &maxSize, sizeof(int));
	  offset += sizeof(int);
}

void RelationManager::createTablerd(vector<Attribute> &recordDescriptor)
{
	if (!recordDescriptor.empty())
	{
		recordDescriptor.clear();
	}

    Attribute attr;
    attr.name = "tableName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "tableType";
    attr.type = TypeInt;
    attr.length = (AttrLength)sizeof(int);
    recordDescriptor.push_back(attr);

    attr.name = "colNum";
    attr.type = TypeInt;
    attr.length = (AttrLength)sizeof(int);
    recordDescriptor.push_back(attr);
}


void RelationManager::createColumnrd(vector<Attribute> &recordDescriptor)
{
	if (!recordDescriptor.empty())
	{
		recordDescriptor.clear();
	}

    Attribute attr;
    attr.name = "tableName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "colName";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)30;
    recordDescriptor.push_back(attr);

    attr.name = "colType";
    attr.type = TypeInt;
    attr.length = (AttrLength)sizeof(int);
    recordDescriptor.push_back(attr);

    attr.name = "colPosition";
    attr.type = TypeInt;
    attr.length = (AttrLength)sizeof(int);
    recordDescriptor.push_back(attr);

    attr.name = "colLength";
    attr.type = TypeInt;
    attr.length = (AttrLength)sizeof(int);
    recordDescriptor.push_back(attr);

}



