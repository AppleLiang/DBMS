
#include "rbfm.h"
#include "pfm.h"
#include <stdio.h>
#include <string.h>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName)
{
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName)
{
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->openFile(fileName.c_str(),fileHandle);

}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle)
{
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->closeFile(fileHandle);
}

bool RecordBasedFileManager::fileExists(const string &fileName)
{
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->FileExists(fileName.c_str());

}

/*RC RecordBasedFileManager::insertRecordIntoPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, int* fieldPosition, int recordLength, RID &rid, int freePage)
{
	int fieldNum = recordDescriptor.size();
	void *buffer = calloc(1,PAGE_SIZE);
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int newSlotEntryaddr;
	if(freePage == -1)
		//if no page in the empty list can contain the record, append a new page
	{
		rid.pageNum = fileHandle.getNumberOfPages();
		*((int*) ((char*) buffer + freeOffsetaddr)) = 0;
		*((int*) ((char*) buffer + freeSpaceaddr)) = 4096;
		*((int*) ((char*) buffer + slotNumaddr)) = 0;
		*((int*) ((char*) buffer + FULL_FLAG)) = 0;
		fileHandle.appendPage(buffer);
		fileHandle.addPageToList(rid.pageNum, EmptyList);
	}
	else
	{
		rid.pageNum = (PageNum) freePage;
	}
	fileHandle.readPage(rid.pageNum, buffer);

		//cout<<"present page num is "<< rid.pageNum<<endl;
	int freeOffset = *((int*) ((char*) buffer + freeOffsetaddr));
	int totalRecordNum = *((int*) ((char*) buffer + slotNumaddr));
	newSlotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(totalRecordNum+1);
	recordDirectoryEntry* newEntry = (recordDirectoryEntry*) ((char*) buffer + newSlotEntryaddr);
	recordDirectoryEntry entry;
	entry.slot_offset = freeOffset;
	entry.slot_num = totalRecordNum;
	entry.slot_length = recordLength + sizeof(int) * fieldNum;
	//cout<<"the length of record is"<<entry.slot_length<<endl;
	//cout<<"the free space offset is"<<entry.slot_offset<<endl;
	rid.slotNum = totalRecordNum;
	memcpy((char*) buffer + newSlotEntryaddr,&entry,sizeof(recordDirectoryEntry));
	memcpy((char*) buffer + newEntry->slot_offset, fieldPosition, fieldNum * sizeof(int)); // copy tuple
	memcpy((char*) buffer + newEntry->slot_offset + fieldNum * sizeof(int), data, newEntry->slot_length-fieldNum*sizeof(int));
	int* pToSlotNum = (int*) ((char*)buffer + slotNumaddr);
	*pToSlotNum = *pToSlotNum + 1;
	int* pToFreeoffset = (int*) ((char*)buffer + freeOffsetaddr);
	*pToFreeoffset = *pToFreeoffset + newEntry->slot_length;
	int* pToFreespace = (int*) ((char*)buffer + freeSpaceaddr);
	*pToFreespace = *pToFreespace - newEntry->slot_length - sizeof(recordDirectoryEntry);
	RC rc = fileHandle.writePage(rid.pageNum,buffer);
	free(buffer);
	return rc;
}*/

RC RecordBasedFileManager::scan(FileHandle &fileHandle ,const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
    const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
	int numPages = fileHandle.getNumberOfPages();
	void *buffer = malloc(PAGE_SIZE);
	void *buffer1 = malloc(PAGE_SIZE);
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr  = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int slotEntryaddr;
	int slotNum;
	recordDirectoryEntry* slotEntry;
	RC rc;
	RID id;
	AttrType attrtype;
	for(int i = 0; i < attributeNames.size(); i++)
	{
		rbfm_ScanIterator.projectedAttributes.push_back(attributeNames[i]);
	}
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
		rbfm_ScanIterator.rdDescriptor.push_back(recordDescriptor[i]);
	}
	//rbfm_ScanIterator.scanRbfm = RecordBasedFileManager::instance();
	rbfm_ScanIterator.next = 0;
	for(int i = 0; i < recordDescriptor.size(); i++)
	{
		if(recordDescriptor[i].name == conditionAttribute)
		{
			attrtype = recordDescriptor[i].type;
		}
	}

	for(int i = 0; i < numPages; i++)
	{
		fileHandle.readPage(i,buffer);
		slotNum = *((int*) ((char*) buffer + slotNumaddr));


		for(int j = 0; j < slotNum; j++)
		{
			slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(j + 1);
			recordDirectoryEntry* newEntry = (recordDirectoryEntry*) ((char*) buffer + slotEntryaddr);
			if(newEntry->slot_num - j == 1 && newEntry->slot_length > 0)
			{
				continue;
			}
			if(newEntry->slot_length > DELETED)
			{
				if(newEntry->slot_length == TOMB_STONE)
				{
					id.pageNum = newEntry->slot_num;
					id.slotNum = newEntry->slot_offset;
				}
				else
				{
				id.pageNum = i;
				id.slotNum = j;
				}
				rc = readAttribute(fileHandle, recordDescriptor, id, conditionAttribute, buffer1);
				if(rc != 0)
				{
					free(buffer);
					free(buffer1);
					return -1;
				}
				bool flag = judgeAttribute(value, buffer1, compOp, attrtype);
				if(flag)
				{
					RID newid;
					newid.pageNum = i;
					newid.slotNum = j;
					rbfm_ScanIterator.recordId.push_back(newid);
				}
			}
		}
	}
	free(buffer);
	free(buffer1);
	return 0;
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid)
{
	int fieldNum = recordDescriptor.size();
	int minRecordLength;
	int* fieldPosition = new int[fieldNum];
	int recordLength = getRecordLength(recordDescriptor,data,fieldPosition,&minRecordLength);
	//cout<<"exit from getrecord length"<<endl;
	int freePage = findPage(fileHandle, recordDescriptor, recordLength+sizeof(int)*fieldNum+sizeof(int), minRecordLength);
	//cout<<"exit from find page"<<endl;
	void *buffer = calloc(1,PAGE_SIZE);
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int newSlotEntryaddr;

	if(freePage == -1)
	//if no page in the empty list can contain the record, append a new page
	{

		rid.pageNum = fileHandle.getNumberOfPages();
		*((int*) ((char*) buffer + freeOffsetaddr)) = 0;
		*((int*) ((char*) buffer + slotNumaddr)) = 0;
		*((int*) ((char*) buffer + freeSpaceaddr)) = 4096 - FREE_ENTRY_OFFSET;
		*((int*) ((char*) buffer + fullFlagaddr)) = 0;
		fileHandle.appendPage(buffer);
		fileHandle.addPageToList(rid.pageNum, EmptyList);
	}
	else
	{
		rid.pageNum = (PageNum) freePage;
	}
	//cout<<"the page found for record is "<<freePage<<endl;
	fileHandle.readPage(rid.pageNum, buffer);

	//cout<<"present page num is "<< rid.pageNum<<endl;

	int freeOffset = *((int*) ((char*) buffer + freeOffsetaddr));
	int freeSpace = *(int*) ((char*)buffer + freeSpaceaddr);
	int totalRecordNum = *((int*) ((char*) buffer + slotNumaddr));
	newSlotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(totalRecordNum+1);

	recordDirectoryEntry* newEntry = (recordDirectoryEntry*) ((char*) buffer + newSlotEntryaddr);
	recordDirectoryEntry entry;
	entry.slot_offset = freeOffset;
	entry.slot_num = totalRecordNum;
	entry.slot_length = recordLength + sizeof(int) * fieldNum + sizeof(int);
	//cout<<"the length of record is"<<entry.slot_length + sizeof(recordDirectoryEntry)<<endl;
	//cout<<"the free space offset is"<<entry.slot_offset<<endl;
	//cout<<"the free space amount is"<<freeSpace<<endl;
	rid.slotNum = totalRecordNum;

	memcpy((char*) buffer + newSlotEntryaddr,&entry,sizeof(recordDirectoryEntry));
	memcpy((char*) buffer + newEntry->slot_offset, &fieldNum, sizeof(int));
	memcpy((char*) buffer + newEntry->slot_offset + sizeof(int), fieldPosition, fieldNum * sizeof(int)); // copy tuple
	memcpy((char*) buffer + newEntry->slot_offset + fieldNum * sizeof(int) + sizeof(int), data, newEntry->slot_length-fieldNum*sizeof(int)-sizeof(int));

	int * pToSlotNum = (int*) ((char*)buffer + slotNumaddr);
	*pToSlotNum = *pToSlotNum + 1;
	int * pToFreeOffset = (int*) ((char*)buffer + freeOffsetaddr);
	*pToFreeOffset = *pToFreeOffset + newEntry->slot_length;
	int * pToFreeSpace = (int*) ((char*)buffer + freeSpaceaddr);
	*pToFreeSpace = *pToFreeSpace - newEntry->slot_length - sizeof(recordDirectoryEntry);
	fileHandle.writePage(rid.pageNum,buffer);
	//printRecord(recordDescriptor,data);
	free(buffer);
	delete []fieldPosition;
	return 0;
}

/*RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
{
	int numOfPages = (int)fileHandle.getNumberOfPages();
	if(rid.pageNum > numOfPages - 1)
	{
		return -1;
	}
	int numOfSlots;
	void* pageData = calloc(1, PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);

	int offset = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	numOfSlots = * (int *)((char *) pageData + offset);
	if(rid.slotNum > numOfSlots - 1)
	{
		return -1;
	}

	offset = offset - sizeof(recordDirectoryEntry) * (rid.slotNum + 1);
	recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) pageData + offset);
	if (rde -> slot_length == 0) {
		free(pageData);
		return -1;
	};
	int numOfSlotAtrribute = recordDescriptor.size();
	memcpy(data, (char*) (numOfSlotAtrribute * sizeof(int) + pageData + rde -> slot_offset), rde ->slot_length - numOfSlotAtrribute * sizeof(int));
	free(pageData);

    return 0;
}*/

/*RC RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	if(data == NULL)
	{
		return -1;
	}
	int offset;
	for(int i = 0; i< recordDescriptor.size(); i++)
	{
		AttrType type = recordDescriptor[i].type;
		string name = recordDescriptor[i].name;
		cout<<name<<':';
		offset = *(int*)((char*)data + sizeof(int)*i);
		switch(name)
		{
		case TypeInt:
			int integer = *(int*)((char*)data + offset);
			cout << integer <<' ';
			break;
		}
		case TypeReal:
			float floatNum = *(float*)((char*)data + offset);
			cout << floatNum <<' ';
			offset += sizeof(float);
			break;
		case TypeVarChar:
			int length = *(int*)((char*)data + offset);
			offset += sizeof(int);
			char* temp = (char*)calloc(length * 2, sizeof(char));
			memcpy(temp, (char*)data + offset, length);
			cout << temp <<' ';
			free(temp);
			break;
	}
	cout<<endl;
	return 0;
}*/

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	if(data == NULL)
	return -1;
	int offset = 0;
	vector<Attribute>::const_iterator iterator = recordDescriptor.begin();
	string result = "";
	while(iterator != recordDescriptor.end())
	{
		AttrType type = (*iterator).type;
		string name = (*iterator).name;
		int length = (*iterator).length;
		if(length == 0)
		{
			iterator++;
			continue;
		}
		cout<<name<<':';
		switch(type)
		{
		case TypeInt:
		{
			int integer = *(int*)((char*)data + offset);
			cout << integer<<' ';
			offset += sizeof(int);
			break;
		}
		case TypeReal:
		{
			float floatNum = *(float*)((char*)data + offset);
			cout << floatNum<<' ';
			offset += sizeof(float);
			break;
		}
		case TypeVarChar:
		{
			int length = *(int*)((char*)data + offset);
			offset += sizeof(int);
			char* temp = (char*)calloc(length * 2, sizeof(char));
			memcpy(temp, (char*)data + offset, length);
			string s = temp;
			cout <<s<<' ';
			offset += sizeof(char) * length;
			free(temp);
			break;
		}
		}
		iterator++;
	}
	cout<<endl;
	return 0;
}



RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
//updated by lhb to handle the tomb stone
{
	int fieldNum = recordDescriptor.size();
	int numOfPages = (int)fileHandle.getNumberOfPages();
	RID newId;
	if(rid.pageNum >= numOfPages)
	{
		return -1;
	}

	void* pageData = calloc(1, PAGE_SIZE);
	void* outdata = calloc(1,PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);

	int offset = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int numOfSlots = * (int *)((char *) pageData + offset);
	if(rid.slotNum >= numOfSlots)
	{
		free(pageData);
		return -1;
	}


	offset = PAGE_SIZE - sizeof(recordDirectoryEntry) * (rid.slotNum + 1)- FREE_ENTRY_OFFSET;

	recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) pageData + offset);
	if (rde->slot_length == DELETED)
	{
		free(pageData);
		return -1;
	}
	else if(rde->slot_length == TOMB_STONE)
	{
		newId.pageNum = rde->slot_num;
		newId.slotNum = rde->slot_offset;
		free(pageData);
		return readRecord(fileHandle, recordDescriptor, newId, data);
	}
	//cout<<"record offset is"<<rde->slot_offset<<endl;
	memcpy(outdata, (char*) pageData + rde -> slot_offset, rde ->slot_length);
	int attrNum = *((int*)outdata);
	int offset1 = 0;
	int fieldoffset;
	int len;
	int nullint = 0;
	float nullfloat = 0;
	string nullstr = "null";
	int strlen;
	//cout<<" attribute num in record is "<<attrNum <<endl;
	//cout<<" record descriptor size is "<<fieldNum<<endl;
	for (int i = 0; i < fieldNum; i++)
	{
		if(i >= attrNum)
		{
			switch (recordDescriptor[i].type)
			{
			case TypeInt:

				memcpy((char*)data + offset1, &nullint, sizeof(int));
				offset1 += sizeof(int);
				break;
			case TypeReal:

				memcpy((char*)data + offset1, &nullfloat, sizeof(float));
				offset1 += sizeof(float);
				break;
			case TypeVarChar:

				strlen = (int)nullstr.size();
				memcpy((char*)data + offset1, &strlen, sizeof(int));
				offset1 += sizeof(int);
				memcpy((char*)data + offset1, nullstr.c_str(), strlen);
				offset1 += strlen;
				break;
			default:
				break;
			}

		}
		if(recordDescriptor[i].length == 0)
		{
			continue;
		}
		//cout<<"enter real data read"<<endl;
		fieldoffset = *((int*) ((char*)outdata + sizeof(int)*i + sizeof(int)));
		//cout<<i<<" data offset is"<<fieldoffset<<endl;
		 switch(recordDescriptor[i].type)
		 {
		 case TypeInt:
			 len = sizeof(int);
			 break;
		 case TypeVarChar:
			 len = *((int*) ((char*)outdata + fieldoffset)) + sizeof(int);
			 break;
		 case TypeReal:
			 len = sizeof(float);
			 break;
		 }
		 memcpy((char*)data + offset1, (char*)outdata + fieldoffset, len);
		 offset1 += len;
	}
	//printRecord(recordDescriptor,data);
	free(pageData);
	free(outdata);

    return 0;
}
/*RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data)
//updated by lhb to handle the tomb stone
{
	int fieldNum = recordDescriptor.size();
	int numOfPages = (int)fileHandle.getNumberOfPages();
	RID newId;
	if(rid.pageNum >= numOfPages)
	{
		return -1;
	}

	void* pageData = calloc(1, PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);

	int offset = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int numOfSlots = * (int *)((char *) pageData + offset);
	if(rid.slotNum >= numOfSlots)
	{
		free(pageData);
		return -1;
	}


	offset = PAGE_SIZE - sizeof(recordDirectoryEntry) * (rid.slotNum + 1)- FREE_ENTRY_OFFSET;

	recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) pageData + offset);
	if (rde->slot_length == DELETED)
	{
		free(pageData);
		return -1;
	}
	else if(rde->slot_length == TOMB_STONE)
	{
		newId.pageNum = rde->slot_num;
		newId.slotNum = rde->slot_offset;
		free(pageData);
		return readRecord(fileHandle, recordDescriptor, newId, data);
	}
	//cout<<"record offset is"<<rde->slot_offset<<endl;
	memcpy(data, (char*) pageData + rde -> slot_offset + fieldNum * sizeof(int) + sizeof(int), rde ->slot_length - fieldNum * sizeof(int) - sizeof(int));
	free(pageData);

    return 0;
}*/

/*RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data)
{
	if(data == NULL)
		return -1;
	int i = 0;
	int offset = 0;
	while(i < recordDescriptor.size())
	{
		Attribute attr = recordDescriptor[i];
		if(attr.type == TypeVarChar)
		{
			int l = * (int *) ((char *) offset);
			string* str;
			memcpy(str, (char *) offset + sizeof(int), l);
			cout << str << " " <<endl;
			offset += l + sizeof(int);
		}
		else if(attr.type == TypeInt)
		{
			int m = * (int *)((char *) offset);
			cout << m << " " <<endl;
			offset += attr.length;
		}
		else if(attr.type == TypeReal)
		{
			float f = * (float *)((char *) offset);
			cout << f << " " <<endl;
			offset += attr.length;
		}
	}
    return -1;
}*/


/*RC RecordBasedFileManager::findPage(FileHandle &fileHandle, int recordLength, int minRecordLength)
{
	int freePageNum;
	int currentPageNum;
	fseek(fileHandle.p, HEAD_EMPTY_OFFSET, SEEK_SET);
	fread(&freePageNum, sizeof(int), 1, fileHandle.p);
	void* buffer = malloc(PAGE_SIZE);
	int slotNum;
	int freeSpaceOffset;
	int freeSpaceAmount;
	int nextPageaddr;
	while(freePageNum != -1)
	{
		fileHandle.readPage(freePageNum, buffer);
		int freeEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
		freeSpaceOffset = *((int*) ((char*) buffer + freeEntryaddr));
		int slotNumAddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
		slotNum = *((int*) ((char*) buffer + slotNumAddr));
		freeSpaceAmount = PAGE_SIZE - freeSpaceOffset - FREE_ENTRY_OFFSET - (slotNum+1)*
				sizeof(recordDirectoryEntry);
		if(freeSpaceAmount >= recordLength)
		{
			break;
		}
		nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
		currentPageNum = freePageNum;
		freePageNum = *((int*) ((char*) buffer + nextPageaddr));
		if(freeSpaceAmount < minRecordLength)
		{
			//cout<<"page "<<currentPageNum<<" is full"<<endl;
			fileHandle.removePageFromList(currentPageNum,EmptyList);
			fileHandle.addPageToList(currentPageNum,FullList);
		}
	}
	free(buffer);
	return freePageNum;

}*/

RC RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data,int *fieldOffset,int *minimumLength)
{
	*minimumLength = 0;
	RC recordlength = 0;
	size_t fieldSize = recordDescriptor.size();
	int field = fieldSize * sizeof(int) + sizeof(int);
	int len;
	*minimumLength += field;
	for (int i = 0; i < recordDescriptor.size(); i++)
	{
		if(recordDescriptor[i].length == 0)
		{
			fieldOffset[i] = -1;
			continue;
		}
		fieldOffset[i] = field;
		switch (recordDescriptor[i].type)
		{
		case TypeInt:
			len = sizeof(int);
			recordlength += sizeof(int);
			*minimumLength += sizeof(int);
			break;
		case TypeReal:
			len= sizeof(float);
			recordlength += sizeof(float);
			*minimumLength += sizeof(float);
			break;
		case TypeVarChar:
			len = (sizeof(int) + *((int*) ((char*) data + recordlength)));
			recordlength += len;
			*minimumLength += sizeof(int);
			break;
		default:
			break;
		}
		field += len;
	}
	*minimumLength += sizeof(recordDirectoryEntry);
	return recordlength;
}


RC RecordBasedFileManager::findPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, int recordLength, int minRecordLength)
{
	int freePageNum;
	int currentPageNum;
	fseek(fileHandle.p, HEAD_EMPTY_OFFSET, SEEK_SET);
	fread(&freePageNum, sizeof(int), 1, fileHandle.p);
	void* buffer = malloc(PAGE_SIZE);
	int slotNum;
	int freeOffset;
	int freeSpaceAmount;
	int nextPageaddr;
	int calculatedSpace;
	//cout<<"present record length is "<<recordLength<<endl;
	while(freePageNum != -1)
	{
		if(1)
		{
			//cout<<"page num in empty list is "<<freePageNum<<endl;
			fileHandle.readPage(freePageNum, buffer);
			int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
			int freeSpaceaddr = PAGE_SIZE -FREE_SPACE_OFFSET;
			int slotNumAddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
			freeOffset = *((int*) ((char*) buffer + freeOffsetaddr));
			freeSpaceAmount = *((int*) ((char*) buffer + freeSpaceaddr));
			slotNum = *((int*) ((char*) buffer + slotNumAddr));
			//cout<<"present record length is "<<recordLength+sizeof(recordDirectoryEntry)<<endl;
			//cout<<"the free space amount in page"<<freePageNum<<" is "<<freeSpaceAmount<<endl;
			//cout<<"the min record length is "<<minRecordLength<<endl;
			if(freeSpaceAmount >= (recordLength + sizeof(recordDirectoryEntry)))
			{
				calculatedSpace = PAGE_SIZE - FREE_ENTRY_OFFSET - freeOffset - (slotNum + 1) * sizeof(recordDirectoryEntry);
				//cout<<"the calculated Space is "<<calculatedSpace<<endl;
				if(calculatedSpace < recordLength)
				{
					reorganizePage(fileHandle, recordDescriptor, freePageNum);
					//cout<<"exit from reorganize page"<<endl;
				}
				break;
			}
			nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
			currentPageNum = freePageNum;
			freePageNum = *((int*) ((char*) buffer + nextPageaddr));
			/*if(freePageNum == currentPageNum)
			{
				while(1)
				{
					1;
				}
			}*/
			//cout<<"the next page is "<<freePageNum<<endl;
			if(freeSpaceAmount < (minRecordLength + sizeof(recordDirectoryEntry)))
			{
				fileHandle.removePageFromList(currentPageNum,EmptyList);
				fileHandle.addPageToList(currentPageNum,FullList);
			}
		}
	}
	free(buffer);
	return freePageNum;
}


 RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
 {
	 void *buffer = malloc(PAGE_SIZE);
	 RC rc = readRecord(fileHandle,recordDescriptor,rid,buffer);
	 if(rc != 0)
	 {
		 return -1;
	 }
	 AttrType attrType;
	 int position;
	 int fieldNum = recordDescriptor.size();
	 int offset = 0;
	 int len;
	 if(rc == 0)
	 {
		 for(int i = 0; i < fieldNum; i++)
		 {
			 if(recordDescriptor[i].length == 0)
			 {
				 continue;
			 }
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
			 if(recordDescriptor[i].name == attributeName)
			 {
				 break;
			 }
			 offset += len;
		 }
		 memcpy(data, (char*)buffer + offset, len);

		 free(buffer);
		 return 0;
	 }
	 free(buffer);
	 return -1;
 }
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr  = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int pageNum = fileHandle.getNumberOfPages();
	void* data = malloc(PAGE_SIZE);
	for(int i = 0; i < pageNum; i++)
	{
		fileHandle.readPage(i,data);
		*((int*) ((char*)data + slotNumaddr)) = 0;
		*((int*) ((char*)data + freeOffsetaddr)) = 0;
		*((int*) ((char*)data + freeSpaceaddr)) = PAGE_SIZE  - FREE_ENTRY_OFFSET;
		int *fullFlag = (int*) ((char*)data + fullFlagaddr);
		fileHandle.writePage(i,data);
		if((*fullFlag) == 1)
		{
			fileHandle.removePageFromList(i,FullList);
			fileHandle.addPageToList(i,EmptyList);
		}
	}
	free(data);
	return 0;

}
 RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
 {
	 int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	 int freeSpaceaddr  = PAGE_SIZE - FREE_SPACE_OFFSET;
	 int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	 int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	 int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	 int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	 int slotEntryaddr;
	 int tombState;
	 RID tupleId;
	 RC rc;

	 void *data = malloc(PAGE_SIZE);
	 rc = fileHandle.readPage(rid.pageNum,data);
	 if(rc != 0)
	 {
		 return -1;
	 }
	 int totalNum = *(int*) ((char*)data + slotNumaddr);
	 int *freeSpace = (int*) ((char*)data + freeSpaceaddr);
	 if(rid.slotNum >= totalNum)
	 {
		 return -1;
	 }

	 slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(rid.slotNum + 1);
	 recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) data + slotEntryaddr);
	 if(rde->slot_length == DELETED)
	 {
		 return -1;
	 }

	 if(rde->slot_length == TOMB_STONE)
	//if this tuple is a tombstone, need to delete both the real tuple and tombstone
	 {
		 tupleId.pageNum = rde->slot_num;
		 tupleId.slotNum = rde->slot_offset;
		 rc = deleteRecord(fileHandle, recordDescriptor, tupleId);

	 }
	 fileHandle.readPage(rid.pageNum,data);
	 slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(rid.slotNum + 1);
	 rde = (recordDirectoryEntry*) ((char*) data + slotEntryaddr);
	 freeSpace = (int*) ((char*)data + freeSpaceaddr);
	 if(rde->slot_length > TOMB_STONE)
	 {
		 *freeSpace += rde->slot_length;
	 }
	 tombState = rde->slot_length;
	 rde->slot_length = DELETED;
	 rc = fileHandle.writePage(rid.pageNum,data);
	 int *fullFlag = (int*) ((char*)data + fullFlagaddr);
	 if((*fullFlag == 1) && tombState > 0)
	 {
		 fileHandle.removePageFromList(rid.pageNum,FullList);
		 fileHandle.addPageToList(rid.pageNum,EmptyList);
	 }

	 free(data);
	 return rc;

 }

 RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
 {
	 int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	 int freeSpaceaddr  = PAGE_SIZE - FREE_SPACE_OFFSET;
	 int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	 int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	 int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	 int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	 int slotEntryaddr;
	 int fieldNum = recordDescriptor.size();
	 RC rc;
	 RID recordId;

	 int* fieldPosition = new int[fieldNum];
	 void *buffer = malloc(PAGE_SIZE);
	 void *buffer1 = malloc(PAGE_SIZE);
	 int* freeSpace;
	 int* fullFlag;
	 rc = fileHandle.readPage(rid.pageNum, buffer);
	 if(rc != 0)
	 {
		 return -1;
	 }
	 slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(rid.slotNum + 1);
	 recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) buffer + slotEntryaddr);
	 if(rde->slot_length == DELETED)
	 {
		 free(buffer);
		 free(buffer1);
		 free(fieldPosition);
		 return -1;
	 }
	 if(rde->slot_length == TOMB_STONE)
	 {
		 recordId.pageNum = rde->slot_num;
		 recordId.slotNum = rde->slot_offset;
		 rc = updateRecord(fileHandle, recordDescriptor, data, recordId);
	 }
	 else
	 {
		 freeSpace = (int*) ((char*)buffer + freeSpaceaddr);
		 int minLength;
		 int recordLength = getRecordLength(recordDescriptor, data, fieldPosition, &minLength) + fieldNum * sizeof(int) + sizeof(int);
		 if(recordLength <= rde->slot_length)
		 {
			 int offset = rde->slot_offset;
			 memcpy((char*) buffer + offset, &fieldNum, sizeof(int));
			 memcpy((char*) buffer + offset + sizeof(int), fieldPosition, fieldNum * sizeof(int));
			 memcpy((char*) buffer + offset + fieldNum * sizeof(int) + sizeof(int), data, recordLength - fieldNum * sizeof(int) - sizeof(int));
			 *freeSpace += rde->slot_length - recordLength;
			 rde->slot_length = recordLength;
			 fullFlag = (int*) ((char*) buffer + fullFlagaddr);
			 rc = 0;
		 }
		 else
		 {
			 RID newId;
			 rc = insertRecord(fileHandle, recordDescriptor, data, newId);
			 if(rc != 0)
			 {
				 return -1;
			 }
			 rc = fileHandle.readPage(newId.pageNum, buffer1);
			 if(rc != 0)
			 {
				 return -1;
			 }
			 int slotEntryaddr1 = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(newId.slotNum + 1);
			 recordDirectoryEntry* rde1 = (recordDirectoryEntry*) ((char*) buffer1 + slotEntryaddr1);
			 rde1->slot_num += 1;
			 rc = fileHandle.writePage(newId.pageNum, buffer1);
			 rc = fileHandle.readPage(rid.pageNum, buffer);
			 freeSpace = (int*) ((char*)buffer + freeSpaceaddr);
			 fullFlag = (int*) ((char*) buffer + fullFlagaddr);
			 slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(rid.slotNum + 1);
			 rde = (recordDirectoryEntry*) ((char*) buffer + slotEntryaddr);
			 *freeSpace += rde->slot_length;
			 rde->slot_length = TOMB_STONE;
			 rde->slot_num = newId.pageNum;
			 rde->slot_offset = newId.slotNum;
		 }
		 rc = fileHandle.writePage(rid.pageNum, buffer);
		 if((*fullFlag) == 1 && (*freeSpace) > (minLength + sizeof(recordDirectoryEntry)))
		 {
			 fileHandle.removePageFromList(rid.pageNum, FullList);
			 fileHandle.addPageToList(rid.pageNum,EmptyList);
		 }
	 }
	 free(buffer);
	 free(buffer1);
	 delete []fieldPosition;
	 return rc;
 }

 RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber)
 {
	 int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	 int freeSpaceaddr  = PAGE_SIZE - FREE_SPACE_OFFSET;
	 int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	 int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	 int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	 int slotEntryaddr;

	 int offset;

	 void *data = malloc(PAGE_SIZE);
	 fileHandle.readPage(pageNumber,data);
	 int totalNum = *(int*) ((char*)data + slotNumaddr);
	 int* freeOffset = (int*) ((char*)data + freeOffsetaddr);
	 int* freeSpace = (int*) ((char*)data + freeSpaceaddr);
	 int recordNum;
	 int upRecord;
	 int offsetChange;
	 int moveOffset;
	 offset = 0;
	 for(recordNum = 0; recordNum < totalNum ; recordNum++)
	 {
		 slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(recordNum + 1);
		 recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*) data + slotEntryaddr);
		 if(rde->slot_length > 0)
		 {
			 if(offset == rde->slot_offset)
			 {
				 offset += rde->slot_length;
				 continue;
			 }
			 //else if(offset > rde->slot_offset)
			 //{
				 /*free(data);
				 cerr<<"file reorganization fails1"<<endl;
				 return -1;*/
			 //}
			 else
			 {
				 memcpy((char*)data + offset, (char*)data + rde->slot_offset, rde->slot_length);
				 rde->slot_offset = offset;
				 offset += rde->slot_length;
			 }
		 }
	 }
	 *freeOffset = offset;
	 int calculatedSpace = PAGE_SIZE - FREE_ENTRY_OFFSET - totalNum * sizeof(recordDirectoryEntry) - offset;
	 //cout<<calculatedSpace<<endl;
	 //cout<<*freeSpace<<endl;
	 /*if(*freeSpace != calculatedSpace)
	 {
		 free(data);
		 cerr<<"file reorganization fails 2"<<endl;
		 return -1;
	 }*/
	 fileHandle.writePage(pageNumber,data);
	 free(data);
	 return 0;
 }

 bool RecordBasedFileManager::judgeAttribute(const void* condition, void* attribute, CompOp compOp, AttrType type)
 {
	 if (compOp == NO_OP)
	 {
	 		return true;
	 }

	 	int result;

	 	switch (type) {
	 	case TypeVarChar:
	 	{
	 		int strLength = *((int*) condition);
	 		string conditionStr = string((char*) ((char*)condition + sizeof(int)), strLength);
	 		strLength = *((int*) attribute);
	 		string attrstr = string((char*) ((char*)attribute + sizeof(int)), strLength);
	 		result = attrstr.compare(conditionStr);
	 		break;
	 	}
	 	case TypeInt:
	 		result = (*((int*) attribute)) - (*((int*) condition));
	 		break;
	 	case TypeReal:
	 		// assume TypeReal is float
	 		result = (*((float*) attribute)) - (*((float*) condition));
	 		break;
	 	}

		switch (compOp)
		{
		case EQ_OP:
			if(result == 0)
				return true;
			return false;
			break;
		case LT_OP:
			if(result < 0)
				return true;
			return false;
			break;
		case GT_OP:
			if(result > 0)
				return true;
			return false;
		case LE_OP:
			if(result <= 0)
				return true;
			return false;
		case GE_OP:
			if(result >= 0)
				return true;
			return false;
		case NE_OP:
			if(result != 0)
				return true;
			return false;
		}

 }

 RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
   {
 	  int len;
 	  int offset;
 	  int offset1;
 	  AttrType colType;
 	  void* buffer = malloc(PAGE_SIZE);
 	  if(next < recordId.size())
 	  {
 		 rid.pageNum = recordId[next].pageNum;
 		 rid.slotNum = recordId[next].slotNum;
 		 RecordBasedFileManager* scanRbfm = RecordBasedFileManager::instance();
 		  scanRbfm->readRecord(handle, rdDescriptor, recordId[next], buffer);
 		  //scanRbfm->printRecord(rdDescriptor,buffer);
 		 // cout<<" print record in get next tuple"<<endl;
 		  offset1 = 0;//offset of projected attributes
 		  for(int i = 0; i < projectedAttributes.size(); i++)
 		  {
 			  offset = 0;//offset of projected attributes in record
 			  for(int j = 0; j < rdDescriptor.size(); j++)
 			  {
 				  if(rdDescriptor[j].length == 0)
 				  {
 					  continue;
 				  }
 				  switch(rdDescriptor[j].type)
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
 				  if(rdDescriptor[j].name == projectedAttributes[i])
 				  {
 					  memcpy((char*)data + offset1, (char*)buffer + offset, len);
 					  offset1 += len;
 					  break;
 				  }
 				  offset += len;
 			  }

 		  }
 		  next++;
 		  free(buffer);
 		  return 0;
 	  }
 	  free(buffer);
 	  return RBFM_EOF;
   };





