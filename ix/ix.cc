
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	 PagedFileManager* pfm = PagedFileManager::instance();
	 return pfm->createFile(fileName.c_str());
}

RC IndexManager::destroyFile(const string &fileName)
{
	 PagedFileManager* pfm = PagedFileManager::instance();
	 return pfm->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	 PagedFileManager* pfm = PagedFileManager::instance();
	 return pfm->openFile(fileName.c_str(),fileHandle);
}

RC IndexManager::closeFile(FileHandle &fileHandle)
{
	 PagedFileManager* pfm = PagedFileManager::instance();
	 return pfm->closeFile(fileHandle);
}

RC IndexManager::insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	// In this function, we need to find the first page and follow the index to get into the page we want to insert this entry to. And
	// we used the below function to insert this record. We use empty list start to store our first page number
	int firstPageNum = fileHandle.getFirstPageNumber();
	int totalPageNum = fileHandle.getNumberOfPages();
	// trace is used to track the trace we search
	vector<int> trace;
	if (totalPageNum == 0) {
		// we build a new page
		void* newPageBuffer = calloc(1, PAGE_SIZE);
		// change the parameters of this page
		int * pToSlotNum = (int*)((char*)newPageBuffer + slotNumaddr);
		*pToSlotNum = 0;
		int * pToFreeOffset = (int*)((char*)newPageBuffer + freeOffsetaddr);
		*pToFreeOffset = 0;
		int * pToFreeSpace = (int*)((char*)newPageBuffer + freeSpaceaddr);
		*pToFreeSpace = freeOffsetaddr - *pToFreeOffset;
		int * pToNextPage = (int*)((char*)newPageBuffer + nextPageaddr);
		*pToNextPage = -1;
		int * pToPrevPage = (int*)((char*)newPageBuffer + prevPageaddr);
		*pToPrevPage = -1;
		int * pToFullFlag = (int*)((char*)newPageBuffer + fullFlagaddr);
		*pToFullFlag = -1;
		fileHandle.appendPage(newPageBuffer);
		fileHandle.setFirstPageNumber(0);
		free(newPageBuffer);
		trace.push_back(0);
		insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
	}
	else if (totalPageNum == 1) {
		trace.push_back(0);
		reorganizePage(fileHandle,trace);
		insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
	}
	else {
		AttrType type = attribute.type;
		void* tempBuffer = calloc(1, PAGE_SIZE);
		fileHandle.readPage(firstPageNum, tempBuffer);
		// put the first page num into trace
		trace.push_back(firstPageNum);
		// get leaf flag
		int flag = *(int*)((char*)tempBuffer + fullFlagaddr);
		int nextPageNum = -1;
		while (flag != -1) {
			int totalSlotNum = *(int*)((char*)tempBuffer + slotNumaddr);
			int position = findInsertPosition(tempBuffer, type, totalSlotNum, key);
			// if position equals -1, we have to check the leftmost pointer, which is the full flag
			if (position == -1) {
				nextPageNum = *(int*)((char*)tempBuffer + fullFlagaddr);
			}
			// else, we read the record on the position
			else {
				recordDirectoryEntry* entryOfIndexRecord = findEntry(tempBuffer, position);
				int indexRecordOffset = entryOfIndexRecord->slot_offset;
				int indexRecordLength = entryOfIndexRecord->slot_length;
				RID* indexRid;
				indexRid = (RID*)((char*)tempBuffer + indexRecordOffset + indexRecordLength - sizeof(RID));
				nextPageNum = indexRid->pageNum;
			}
			// read next page
			fileHandle.readPage(nextPageNum, tempBuffer);
			// put the next page num into trace
			trace.push_back(nextPageNum);
			// get the flag of the next page
			flag = *(int*)((char*)tempBuffer + fullFlagaddr);

		}	
		free(tempBuffer);
		reorganizePage(fileHandle,trace);
		insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
	}
	return 0;
}
RC IndexManager::insertEntryToPage (FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid, vector<int> &trace, int flag)
{
	//read page first
	void *buffer = calloc(1,PAGE_SIZE);
	int  pageNum = trace.back();
	trace.pop_back();
	fileHandle.readPage(pageNum, buffer);

	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;

	// we have to get the size of the insert entry
	int sizeOfInsertEntry = 0;
	int sizeOfVarCharKey = 0;
	AttrType type = attribute.type;
	switch (type)
	{
	case TypeInt:
		sizeOfInsertEntry = sizeof(int) + sizeof(RID);
		break;
	case TypeReal:
		sizeOfInsertEntry = sizeof(float) + sizeof(RID);
		break;
	case TypeVarChar:
		sizeOfVarCharKey = * ((int*) (char*) key);
		sizeOfInsertEntry = sizeof(int) + sizeof(RID) + sizeOfVarCharKey;
		break;
	}
	// put key and rid into data
	void *data = calloc(1, sizeOfInsertEntry);
	switch (type)
	{
	case TypeInt:
		memcpy((char*)data, key, sizeof(int));
		memcpy((char*)data + sizeof(int), &rid, sizeof(RID));
		break;
	case TypeReal:
		memcpy((char*)data, key, sizeof(float));
		memcpy((char*)data + sizeof(float), &rid, sizeof(RID));
		break;
	case TypeVarChar:
		sizeOfVarCharKey = * ((int*) (char*) key);
		memcpy((char*)data, key, sizeof(int) + sizeOfVarCharKey);
		memcpy((char*)data + sizeof(int) + sizeOfVarCharKey, &rid, sizeof(RID));
		break;
	}
	// check whether it has enough space
	int freeSpaceOffset = *((int*)((char*)buffer + freeOffsetaddr));
	int freeSpace = *((int*) ((char*)buffer + freeSpaceaddr));
	int totalRecordNum = *((int*) ((char*) buffer + slotNumaddr));
	int insertPosition = findInsertPosition(buffer, type, totalRecordNum, key);
	if (sizeOfInsertEntry + sizeof(recordDirectoryEntry) > freeSpace) {
		return splitPage(fileHandle, buffer, attribute, totalRecordNum, key, rid, pageNum, trace, flag);
	}
	else{
		insertToPosition(fileHandle, buffer, data, insertPosition, sizeOfInsertEntry, totalRecordNum, pageNum);
	}
	return 0;
}
RC IndexManager::splitPage(FileHandle &fileHandle, void* buffer, const Attribute &attribute, int total, const void *key,
  		  const RID &rid, int pageNum, vector<int> &trace, int flag)
{
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	AttrType type = attribute.type;
	if(flag == -1)
	{
		int originalInsertPosition = findInsertPosition(buffer, type, total, key);
		int midPosition = total / 2;
		recordDirectoryEntry* midEntry = findEntry(buffer, midPosition);
		int midOffset = midEntry->slot_offset;
		recordDirectoryEntry* lastEntry = findEntry(buffer, total - 1);
		int lastOffset = lastEntry->slot_offset;
		int lastLength = lastEntry->slot_length;
		void* newKey = calloc(1, midEntry->slot_length - sizeof(RID));
		memcpy(newKey, (char*)buffer + midEntry->slot_offset, midEntry->slot_length - sizeof(RID));
		RID newRid;
		newRid.slotNum = -1;
		// build a new page and put the last half into it
		// copy data
		void* newBuffer = calloc(1, PAGE_SIZE);
		memcpy(newBuffer, (char*)buffer + midOffset, lastOffset - midOffset + lastLength);
		// copy slot entries
		memcpy((char*)newBuffer + freeOffsetaddr - (total - midPosition) * sizeof(recordDirectoryEntry),
				(char*)buffer + freeOffsetaddr - total * sizeof(recordDirectoryEntry), (total - midPosition) * sizeof(recordDirectoryEntry));
		// modify all these new slot entries
		for (int slotNum = 0; slotNum < (total - midPosition); slotNum++)
		{
			int slotEntryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)* (slotNum + 1);
			((recordDirectoryEntry*)((char*)newBuffer + slotEntryAddr))->slot_num -= midPosition;
			((recordDirectoryEntry*)((char*)newBuffer + slotEntryAddr))->slot_offset -= midOffset;
		}
		// change parameters of the old pages
		int * pToSlotNum = (int*)((char*)buffer + slotNumaddr);
		*pToSlotNum = *pToSlotNum - (total - midPosition);
		int * pToFreeOffset = (int*)((char*)buffer + freeOffsetaddr);
		*pToFreeOffset = midOffset;
		int * pToFreeSpace = (int*)((char*)buffer + freeSpaceaddr);
		*pToFreeSpace = *pToFreeSpace + (lastOffset - midOffset + lastLength) + (total - midPosition) * sizeof(recordDirectoryEntry);
		int newPageNum = fileHandle.getNumberOfPages();
		newRid.pageNum = newPageNum; //Remember to change to newRid
		int * pToNextPage = (int*)((char*)buffer + nextPageaddr);
		int originalNextPageNum = *pToNextPage;
		*pToNextPage = newPageNum;
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
		// change parameters of the new page
		pToSlotNum = (int*)((char*)newBuffer + slotNumaddr);
		*pToSlotNum = total - midPosition;
		pToFreeOffset = (int*)((char*)newBuffer + freeOffsetaddr);
		*pToFreeOffset = lastOffset - midOffset + lastLength;
		pToFreeSpace = (int*)((char*)newBuffer + freeSpaceaddr);
		*pToFreeSpace = freeOffsetaddr - *pToFreeOffset - (total - midPosition) *sizeof(recordDirectoryEntry);
		pToNextPage = (int*)((char*)newBuffer + nextPageaddr);
		*pToNextPage = originalNextPageNum;
		int * pToPrevPage = (int*)((char*)newBuffer + prevPageaddr);
		*pToPrevPage = -1;
		int * pToFullFlag = (int*)((char*)newBuffer + fullFlagaddr);
		*pToFullFlag = -1;
		fileHandle.appendPage(newBuffer);
		free(newBuffer);
		// Insert the record to one of this two pages
		if (originalInsertPosition >= midPosition)
		{
			// insert in the new page
			int insertPageNum = fileHandle.getNumberOfPages() - 1;
			trace.push_back(insertPageNum);
			insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
		}
		else
		{
			trace.push_back(pageNum);
			insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
		}
		if (trace.size() == 0)
		{
			void* newParentBuffer = calloc(1, PAGE_SIZE);
			// get this new parent page number
			int newParentPageNum = fileHandle.getNumberOfPages();
			// set the new first page number
			fileHandle.setFirstPageNumber(newParentPageNum);
			// change the parameters of this page
			pToSlotNum = (int*)((char*)newParentBuffer + slotNumaddr);
			*pToSlotNum = 0;
			pToFreeOffset = (int*)((char*)newParentBuffer + freeOffsetaddr);
			*pToFreeOffset = 0;
			pToFreeSpace = (int*)((char*)newParentBuffer + freeSpaceaddr);
			*pToFreeSpace = freeOffsetaddr - *pToFreeOffset;
			pToNextPage = (int*)((char*)newParentBuffer + nextPageaddr);
			*pToNextPage = -1;
			pToPrevPage = (int*)((char*)newParentBuffer + prevPageaddr);
			*pToPrevPage = -1;
			pToFullFlag = (int*)((char*)newParentBuffer + fullFlagaddr);
			*pToFullFlag = pageNum;
			fileHandle.appendPage(newParentBuffer);
			//cout << "The leftmost pointer of parent page is" << *(int*)((char*)newParentBuffer + fullFlagaddr) << endl;
			free(newParentBuffer);
			// insert our data into it
			trace.push_back(newParentPageNum);
			insertEntryToPage(fileHandle, attribute, newKey, newRid, trace, pageNum);
			free(newKey);
		}
		else
		{
			insertEntryToPage(fileHandle, attribute, newKey, newRid, trace, pageNum);
			free(newKey);
		}

	}
	else
	{
		// This means you are spliting a non-leaf page
		int originalInsertPosition = findInsertPosition(buffer, type, total, key);
		int midPosition = total / 2;
		recordDirectoryEntry* midEntry = findEntry(buffer, midPosition);
		int midOffset = midEntry->slot_offset;
		int midLength = midEntry->slot_length;
		recordDirectoryEntry* lastEntry = findEntry(buffer, total - 1);
		int lastOffset = lastEntry->slot_offset;
		int lastLength = lastEntry->slot_length;
		// save the middle slot for inserting in the parent page
		void* newKey = calloc(1, midEntry->slot_length - sizeof(RID));
		memcpy(newKey, (char*)buffer + midEntry->slot_offset, midEntry->slot_length - sizeof(RID));
		RID newRid;
		newRid.slotNum = -1;
		RID* idbuf;
		int fullflagnewpage;
		idbuf = (RID*)((char*)buffer + midEntry->slot_offset + midEntry->slot_length - sizeof(RID));
		fullflagnewpage = idbuf->pageNum;
		void* newBuffer = calloc(1, PAGE_SIZE);
		memcpy(newBuffer, (char*)buffer + midOffset + midLength, lastOffset - midOffset + lastLength - midLength);
		// copy slot entries
		memcpy((char*)newBuffer + freeOffsetaddr - (total - midPosition - 1) * sizeof(recordDirectoryEntry),
				(char*)buffer + freeOffsetaddr - total * sizeof(recordDirectoryEntry), (total - midPosition - 1) * sizeof(recordDirectoryEntry));
		// modify all these new slot entries
		for (int slotNum = 0; slotNum < (total - midPosition - 1); slotNum++)
		{
			int slotEntryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)* (slotNum + 1);
			((recordDirectoryEntry*)((char*)newBuffer + slotEntryAddr))->slot_num -= midPosition + 1;
			((recordDirectoryEntry*)((char*)newBuffer + slotEntryAddr))->slot_offset -= midOffset + midLength;
		}
		// change parameters of the old page
		int * pToSlotNum = (int*)((char*)buffer + slotNumaddr);
		*pToSlotNum = *pToSlotNum - (total - midPosition);
		int * pToFreeOffset = (int*)((char*)buffer + freeOffsetaddr);
		*pToFreeOffset = midOffset;
		int * pToFreeSpace = (int*)((char*)buffer + freeSpaceaddr);
		*pToFreeSpace = *pToFreeSpace + (lastOffset - midOffset + lastLength) + (total - midPosition) * sizeof(recordDirectoryEntry);
		int newPageNum = fileHandle.getNumberOfPages();
		newRid.pageNum = newPageNum; //Remember to change to newRid
		int * pToNextPage = (int*)((char*)buffer + nextPageaddr);
		int originalNextPageNum = *pToNextPage;
		*pToNextPage = newPageNum;
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
		// change parameters of the new page
		pToSlotNum = (int*)((char*)newBuffer + slotNumaddr);
		*pToSlotNum = total - midPosition - 1;
		pToFreeOffset = (int*)((char*)newBuffer + freeOffsetaddr);
		*pToFreeOffset = lastOffset - midOffset + lastLength - midLength;
		pToFreeSpace = (int*)((char*)newBuffer + freeSpaceaddr);
		*pToFreeSpace = freeOffsetaddr - *pToFreeOffset - (total - midPosition - 1) *sizeof(recordDirectoryEntry);
		pToNextPage = (int*)((char*)newBuffer + nextPageaddr);
		*pToNextPage = originalNextPageNum;
		int *pToPrevPage = (int*)((char*)newBuffer + prevPageaddr);
		*pToPrevPage = -1;
		int* pToFullFlag = (int*)((char*)newBuffer + fullFlagaddr);
		*pToFullFlag = fullflagnewpage;
		fileHandle.appendPage(newBuffer);
		free(newBuffer);
		// Insert the record to one of this two pages
		if (originalInsertPosition >= midPosition)
		{
			int insertPageNum = fileHandle.getNumberOfPages() - 1;
			trace.push_back(insertPageNum);
			insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
		}
		else
		{
			trace.push_back(pageNum);
			insertEntryToPage(fileHandle, attribute, key, rid, trace, -1);
		}
		// insert middle node to its parent page
		if (trace.size() == 0)
		{
			void* newParentBuffer = calloc(1, PAGE_SIZE);
			// get this new parent page number
			int newParentPageNum = fileHandle.getNumberOfPages();
			// set the new first page number
			fileHandle.setFirstPageNumber(newParentPageNum);
			// change the parameters of this page
			pToSlotNum = (int*)((char*)newParentBuffer + slotNumaddr);
			*pToSlotNum = 0;
			pToFreeOffset = (int*)((char*)newParentBuffer + freeOffsetaddr);
			*pToFreeOffset = 0;
			pToFreeSpace = (int*)((char*)newParentBuffer + freeSpaceaddr);
			*pToFreeSpace = freeOffsetaddr - *pToFreeOffset;
			pToNextPage = (int*)((char*)newParentBuffer + nextPageaddr);
			*pToNextPage = -1;
			pToPrevPage = (int*)((char*)newParentBuffer + prevPageaddr);
			*pToPrevPage = -1;
			pToFullFlag = (int*)((char*)newParentBuffer + fullFlagaddr);
			*pToFullFlag = pageNum;
			fileHandle.appendPage(newParentBuffer);
			free(newParentBuffer);
			// insert our data into it
			trace.push_back(newParentPageNum);
			insertEntryToPage(fileHandle, attribute, newKey, newRid, trace, pageNum);
			free(newKey);
		}
		else
		{
			insertEntryToPage(fileHandle, attribute, newKey, newRid, trace, pageNum);
			free(newKey);
		}
	}
	return 0;
}
recordDirectoryEntry* IndexManager::findEntry(void* buffer, int entryNum) {
	int entryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry) * (entryNum + 1);
	return (recordDirectoryEntry*)((char*)buffer + entryAddr);
}
int IndexManager::findInsertPosition(void* buffer, AttrType type, int total, const void *key) {
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int i = 0;
	for(int i = total; i > 0; i--) {
			if(type == TypeInt) {
				int sizeOfInsertEntry = sizeof(int) + sizeof(RID);
				int keyValue = *(int*)key;
				int compValue = *(int*)((char*) buffer + (i-1) * sizeOfInsertEntry);
				if(keyValue >= compValue) {
					return i-1;
				}
				else {
					continue;
				}
			}
			if(type == TypeReal) {
				int sizeOfInsertEntry = sizeof(float) + sizeof(RID);
				float keyValue = *(float*)key;
				float compValue = *(float*)((char*) buffer + (i-1) * sizeOfInsertEntry);
				if(keyValue >= compValue) {
					return i-1;
				}
				else {
					continue;
				}
			}
			if(type == TypeVarChar) {
				int entryStart = freeOffsetaddr - i * sizeof(recordDirectoryEntry);
				recordDirectoryEntry* currentEntry = (recordDirectoryEntry*)((char*)buffer + entryStart);
				int currentCompValueOffset = currentEntry->slot_offset;
				int currentCompValueLength = *((int*)((char*)buffer + currentCompValueOffset));
				int keyValueLength = *(int*)key;
				string keyValue = string((char*)((char*)key + sizeof(int)), keyValueLength);
				string compValue = string((char*) ((char*)buffer + currentCompValueOffset + sizeof(int)), currentCompValueLength);
				if(keyValue >= compValue) {
						return i-1;
				}
				else {
						continue;
				}
			}
	}
	return i - 1;
}
int IndexManager::findDeletePosition(void* buffer, AttrType type, int total, const void *key) {
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int i = 0;
	for (int i = total; i > 0; i--) {
		if (type == TypeInt) {
			int sizeOfInsertEntry = sizeof(int)+sizeof(RID);
			int keyValue = *(int*)key;
			int compValue = *(int*)((char*)buffer + (i - 1) * sizeOfInsertEntry);
			if (keyValue == compValue) {
				return i - 1;
			}
			else {
				continue;
			}
		}
		if (type == TypeReal) {
			int sizeOfInsertEntry = sizeof(float)+sizeof(RID);
			float keyValue = *(float*)key;
			float compValue = *(float*)((char*)buffer + (i - 1) * sizeOfInsertEntry);
			if (keyValue == compValue) {
				return i - 1;
			}
			else {
				continue;
			}
		}
		if (type == TypeVarChar) {
			int entryStart = freeOffsetaddr - i * sizeof(recordDirectoryEntry);
			recordDirectoryEntry* currentEntry = (recordDirectoryEntry*)(char*)buffer + entryStart;
			int currentCompValueOffset = currentEntry->slot_offset;
			int currentCompValueLength = *((int*)currentCompValueOffset);
			int keyValueLength = *(int*)key;
			string keyValue = string((char*)((char*)key + sizeof(int)), keyValueLength);
			string compValue = string((char*)((char*)buffer + currentCompValueOffset + sizeof(int)), currentCompValueLength);
			if (keyValue == compValue) {
				return i - 1;
			}
			else {
				continue;
			}
		}
	}
	return i - 1;
}
void IndexManager::insertToPosition(FileHandle &fileHandle, void* buffer, void* data, const int insertPosition, const int sizeOfInsertEntry, const int totalRecordNum, int pageNum) {
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int freeSpaceOffset = *(int*)((char*)buffer + freeOffsetaddr);
	// if insert in the end, no need to move data
	if (insertPosition == totalRecordNum - 1) {
		// copy data
		memcpy((char*)buffer + freeSpaceOffset, data, sizeOfInsertEntry);
		//add slot entry
		int newSlotEntryaddr = freeOffsetaddr - sizeof(recordDirectoryEntry)* (totalRecordNum + 1);
		recordDirectoryEntry newEntry;
		newEntry.slot_offset = freeSpaceOffset;
		newEntry.slot_num = totalRecordNum;
		newEntry.slot_length = sizeOfInsertEntry;
		memcpy((char*)buffer + newSlotEntryaddr, &newEntry, sizeof(recordDirectoryEntry));
		//change all parameters
		int * pToSlotNum = (int*)((char*)buffer + slotNumaddr);
		*pToSlotNum = *pToSlotNum + 1;
		int * pToFreeOffset = (int*)((char*)buffer + freeOffsetaddr);
		*pToFreeOffset = *pToFreeOffset + sizeOfInsertEntry;
		int * pToFreeSpace = (int*)((char*)buffer + freeSpaceaddr);
		*pToFreeSpace = *pToFreeSpace - sizeOfInsertEntry - sizeof(recordDirectoryEntry);
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
		free(data);
	}
	// not in the end
	else {
		// find the entry of the next record of the insert position record
		recordDirectoryEntry* insertNextEntry;
		insertNextEntry = findEntry(buffer, insertPosition + 1);
		int insertNextEntryOffset = insertNextEntry->slot_offset;
		int insertNextEntryNum = insertNextEntry->slot_num;
		recordDirectoryEntry* lastEntry;
		lastEntry = findEntry(buffer, totalRecordNum - 1);
		int lastEntryOffset = lastEntry->slot_offset;
		int lastEntryNum = lastEntry->slot_num;
		int lastEntryLength = lastEntry->slot_length;
		int needMovedEntryNum = lastEntryNum - insertNextEntryNum + 1;
		// move back data first. use temp buffer to move
		void* tempBuffer = calloc(1, lastEntryOffset - insertNextEntryOffset + lastEntryLength);
		memcpy(tempBuffer, (char*)buffer + insertNextEntryOffset, lastEntryOffset - insertNextEntryOffset + lastEntryLength);
		memcpy((char*)buffer + insertNextEntryOffset + sizeOfInsertEntry, tempBuffer, lastEntryOffset - insertNextEntryOffset + lastEntryLength);
		free(tempBuffer);
		// put in our data
		memcpy((char*)buffer + insertNextEntryOffset, data, sizeOfInsertEntry);
		//change back rids
		for (int slotNum = insertNextEntryNum; slotNum <= lastEntryNum; slotNum++) {
			int slotEntryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)* (slotNum + 1);
			((recordDirectoryEntry*)((char*)buffer + slotEntryAddr))->slot_num++;
			((recordDirectoryEntry*)((char*)buffer + slotEntryAddr))->slot_offset += sizeOfInsertEntry;
		}
		// move back rids forward. use temp buffer to move
		tempBuffer = calloc(1, needMovedEntryNum * sizeof(recordDirectoryEntry));
		memcpy(tempBuffer, (char*)buffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* totalRecordNum, needMovedEntryNum * sizeof(recordDirectoryEntry));
		memcpy((char*)buffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* (totalRecordNum + 1), tempBuffer, needMovedEntryNum * sizeof(recordDirectoryEntry));
		free(tempBuffer);
		// build a new slot entry
		recordDirectoryEntry newEntry;
		newEntry.slot_offset = insertNextEntryOffset;
		newEntry.slot_num = insertPosition + 1;
		newEntry.slot_length = sizeOfInsertEntry;
		// put it into page
		memcpy((char*)buffer + freeOffsetaddr - sizeof(recordDirectoryEntry)*(insertNextEntryNum + 1), &newEntry, sizeof(recordDirectoryEntry));
		// change other parameters
		int * pToSlotNum = (int*)((char*)buffer + slotNumaddr);
		*pToSlotNum = *pToSlotNum + 1;
		int * pToFreeOffset = (int*)((char*)buffer + freeOffsetaddr);
		*pToFreeOffset = *pToFreeOffset + sizeOfInsertEntry;
		int * pToFreeSpace = (int*)((char*)buffer + freeSpaceaddr);
		*pToFreeSpace = *pToFreeSpace - sizeOfInsertEntry - sizeof(recordDirectoryEntry);
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
		free(data);
	}
}
RC IndexManager::reorganizePage(FileHandle &fileHandle,vector<int>& trace)
{
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	//int firstPageNum = fileHandle.getFirstPageNumber();
	//int totalPageNum = fileHandle.getNumberOfPages();
	int pageNum = trace.back();
	void* buffer = malloc(PAGE_SIZE);
	void* temp = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, buffer);
	memcpy(temp,buffer,PAGE_SIZE);
	int totalSlotNum = *(int*)((char*)buffer + slotNumaddr);
	int offset = 0;
	int* freeOffset = (int*) ((char*)temp + freeOffsetaddr);
	int* freeSpace = (int*) ((char*)temp + freeSpaceaddr);
	int* totalNum = (int*)((char*)temp + slotNumaddr);
	int j = 0;
	for(int i = 0; i<totalSlotNum; i++)
	{
		 int slotEntryaddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(i + 1);
		 recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*)buffer + slotEntryaddr);
		 if(rde->slot_length > -1)
		 {
			 int slotEntryaddr1 = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)*(j + 1);
			 recordDirectoryEntry* rde1 = (recordDirectoryEntry*) ((char*)temp + slotEntryaddr);
			 memcpy((char*)temp + offset, (char*)buffer + rde->slot_offset,rde->slot_length);
			 rde1->slot_length = rde->slot_length;
			 rde1->slot_num = j;
			 rde1->slot_offset = offset;
			 offset += rde->slot_length;
			 j++;
		 }
	}
	*freeOffset = offset;
	*freeSpace = freeOffsetaddr - offset - j * sizeof(recordDirectoryEntry);
	*totalNum = j;
	fileHandle.writePage(pageNum,temp);
	free(buffer);
	free(temp);
	return 0;
}

RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)  // Delete index entry
{
	// we need to find the entry first using the search method in insert
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	void* tempBuffer = calloc(1,PAGE_SIZE);
	RC rc;
	int firstPageNum = fileHandle.getFirstPageNumber();
	int totalPageNum = fileHandle.getNumberOfPages();
	if (totalPageNum == 0) {
		free(tempBuffer);
		return -1;
	}
	else
	{
		AttrType type = attribute.type;
		//void* tempBuffer = calloc(1, PAGE_SIZE);
		fileHandle.readPage(firstPageNum, tempBuffer);
		int lastPageNum = firstPageNum;
		// get leaf flag
		/*int a = *(int*)((char*)tempBuffer + slotNumaddr);
		int b = *(int*)((char*)tempBuffer + prevPageaddr);
		int c = *(int*)((char*)tempBuffer + nextPageaddr);
		int d = *(int*)((char*)tempBuffer + freeSpaceaddr);
		int e = *(int*)((char*)tempBuffer + freeOffsetaddr);*/
		int flag = *(int*)((char*)tempBuffer + fullFlagaddr);
		int nextPageNum = -1;
		while (flag != -1) {
			int totalSlotNum = *(int*)((char*)tempBuffer + slotNumaddr);
			int position = findInsertPosition(tempBuffer, type, totalSlotNum, key);
			// if position equals -1, we have to check the leftmost pointer, which is the full flag
			if (position == -1) {
				nextPageNum = *(int*)((char*)tempBuffer + fullFlagaddr);
			}
			// else, we read the record on the position
			else {
				recordDirectoryEntry* entryOfIndexRecord = findEntry(tempBuffer, position);
				int indexRecordOffset = entryOfIndexRecord->slot_offset;
				int indexRecordLength = entryOfIndexRecord->slot_length;
				nextPageNum = *(int*)((char*)tempBuffer + indexRecordOffset + indexRecordLength - sizeof(RID));
			}
			// updata the last page num
			lastPageNum = nextPageNum;
			// read next page
			fileHandle.readPage(nextPageNum, tempBuffer);
			// get the flag of the next page
			flag = *(int*)((char*)tempBuffer + fullFlagaddr);
		}
		// we get the leaf page num, we could delete the entry in it
		// if there is no record in this page
		int totalSlotNum = *(int*)((char*)tempBuffer + slotNumaddr);
		if (totalSlotNum == 0) {
			rc = -1;
		}
		else
		{
			int deletePosition = findDeletePosition(tempBuffer, type, totalSlotNum, key);
			recordDirectoryEntry* deleteEntry = findEntry(tempBuffer, deletePosition);
			//cout<<"the length of entry is"<<deleteEntry->slot_length<<endl;
			int deleteEntryOffset = deleteEntry->slot_offset;
			int deleteEntryLength = deleteEntry->slot_length;
			if (deletePosition == -1) {
				rc = -1;
			}
			else
			{
				if(deleteEntry->slot_length > -1)
				{
					deleteEntry->slot_length = -1;
					fileHandle.writePage(lastPageNum,tempBuffer);
					rc = 0;
				}
				else
				{
					rc = -1;
				}
				// if the delete position is in the end, no need to move data
				/*if (deletePosition == totalSlotNum - 1) {
					//change parameters
					int * pToSlotNum = (int*)((char*)tempBuffer + slotNumaddr);
					*pToSlotNum = *pToSlotNum - 1;
					int * pToFreeOffset = (int*)((char*)tempBuffer + freeOffsetaddr);
					*pToFreeOffset = deleteEntryOffset;
					int * pToFreeSpace = (int*)((char*)tempBuffer + freeSpaceaddr);
					*pToFreeSpace = *pToFreeSpace + deleteEntryLength +sizeof(recordDirectoryEntry);
					fileHandle.writePage(lastPageNum, tempBuffer);
					free(tempBuffer);
				}
				// the delete position is not in the end
				else
				{
					// find the next entry of our deleting record
					recordDirectoryEntry* deleteNextEntry;
					deleteNextEntry = findEntry(tempBuffer, deletePosition + 1);
					int deleteNextEntryOffset = deleteNextEntry->slot_offset;
					int deleteNextEntryNum = deleteNextEntry->slot_num;
					recordDirectoryEntry* lastEntry;
					lastEntry = findEntry(tempBuffer, totalSlotNum - 1);
					int lastEntryOffset = lastEntry->slot_offset;
					int lastEntryNum = lastEntry->slot_num;
					int lastEntryLength = lastEntry->slot_length;
					int needMovedEntryNum = lastEntryNum - deleteNextEntryNum + 1;
					// move forward data first. use another temp buffer to move
					void* anotherTempBuffer = calloc(1, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					memcpy(anotherTempBuffer, (char*)tempBuffer + deleteNextEntryOffset, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					memcpy((char*)tempBuffer + deleteEntryOffset, anotherTempBuffer, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					free(anotherTempBuffer);
					//change rids
					for (int slotNum = deleteNextEntryNum; slotNum <= lastEntryNum; slotNum++) {
						int slotEntryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)* (slotNum + 1);
						((recordDirectoryEntry*)((char*)tempBuffer + slotEntryAddr))->slot_num--;
						((recordDirectoryEntry*)((char*)tempBuffer + slotEntryAddr))->slot_offset -= deleteEntryLength;
					}
					// move back rids backwards. use another temp buffer to move
					anotherTempBuffer = calloc(1, needMovedEntryNum * sizeof(recordDirectoryEntry));
					memcpy(anotherTempBuffer, (char*)tempBuffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* totalSlotNum, needMovedEntryNum * sizeof(recordDirectoryEntry));
					memcpy((char*)tempBuffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* (totalSlotNum - 1), anotherTempBuffer, needMovedEntryNum * sizeof(recordDirectoryEntry));
					free(anotherTempBuffer);
					// change other parameters
					int * pToSlotNum = (int*)((char*)tempBuffer + slotNumaddr);
					*pToSlotNum = *pToSlotNum - 1;
					int * pToFreeOffset = (int*)((char*)tempBuffer + freeOffsetaddr);
					*pToFreeOffset = *pToFreeOffset - deleteEntryLength;
					int * pToFreeSpace = (int*)((char*)tempBuffer + freeSpaceaddr);
					*pToFreeSpace = *pToFreeSpace + deleteEntryLength + sizeof(recordDirectoryEntry);
					fileHandle.writePage(lastPageNum, tempBuffer);
					free(tempBuffer);*/
				//}
			}
		}
	}
	free(tempBuffer);
	return rc;
}
/*RC IndexManager::deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid)  // Delete index entry
{
	// we need to find the entry first using the search method in insert
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;

	int firstPageNum = fileHandle.getFirstPageNumber();
	int totalPageNum = fileHandle.getNumberOfPages();
	if (totalPageNum == 0) {
		return -1;
	}
	else {
		AttrType type = attribute.type;
		void* tempBuffer = calloc(1, PAGE_SIZE);
		fileHandle.readPage(firstPageNum, tempBuffer);
		int lastPageNum = firstPageNum;
		// get leaf flag
		int flag = *(int*)((char*)tempBuffer + fullFlagaddr);
		int nextPageNum = -1;
		while (flag != -1) {
			int totalSlotNum = *(int*)((char*)tempBuffer + slotNumaddr);
			int position = findInsertPosition(tempBuffer, type, totalSlotNum, key);
			// if position equals -1, we have to check the leftmost pointer, which is the full flag
			if (position == -1) {
				nextPageNum = *(int*)((char*)tempBuffer + fullFlagaddr);
			}
			// else, we read the record on the position
			else {
				recordDirectoryEntry* entryOfIndexRecord = findEntry(tempBuffer, position);
				int indexRecordOffset = entryOfIndexRecord->slot_offset;
				int indexRecordLength = entryOfIndexRecord->slot_length;
				nextPageNum = *(int*)((char*)tempBuffer + indexRecordOffset + indexRecordLength - sizeof(RID));
			}
			// updata the last page num
			lastPageNum = nextPageNum;
			// read next page
			fileHandle.readPage(nextPageNum, tempBuffer);
			// get the flag of the next page
			flag = *(int*)((char*)tempBuffer + fullFlagaddr);
		}
		// we get the leaf page num, we could delete the entry in it
		// if there is no record in this page
		int totalSlotNum = *(int*)((char*)tempBuffer + slotNumaddr);
		if (totalSlotNum == 0) {
			return -1;
		}
		else {
			int deletePosition = findDeletePosition(tempBuffer, type, totalSlotNum, key);
			recordDirectoryEntry* deleteEntry = findEntry(tempBuffer, deletePosition);
			int deleteEntryOffset = deleteEntry->slot_offset;
			int deleteEntryLength = deleteEntry->slot_length;
			if (deletePosition == -1) {
				return -1;
			}
			else {
				// if the delete position is in the end, no need to move data
				if (deletePosition == totalSlotNum - 1) {
					//change parameters
					int * pToSlotNum = (int*)((char*)tempBuffer + slotNumaddr);
					*pToSlotNum = *pToSlotNum - 1;
					int * pToFreeOffset = (int*)((char*)tempBuffer + freeOffsetaddr);
					*pToFreeOffset = deleteEntryOffset;
					int * pToFreeSpace = (int*)((char*)tempBuffer + freeSpaceaddr);
					*pToFreeSpace = *pToFreeSpace + deleteEntryLength +sizeof(recordDirectoryEntry);
					fileHandle.writePage(lastPageNum, tempBuffer);
					free(tempBuffer);
				}
				// the delete position is not in the end
				else {
					// find the next entry of our deleting record
					recordDirectoryEntry* deleteNextEntry;
					deleteNextEntry = findEntry(tempBuffer, deletePosition + 1);
					int deleteNextEntryOffset = deleteNextEntry->slot_offset;
					int deleteNextEntryNum = deleteNextEntry->slot_num;
					recordDirectoryEntry* lastEntry;
					lastEntry = findEntry(tempBuffer, totalSlotNum - 1);
					int lastEntryOffset = lastEntry->slot_offset;
					int lastEntryNum = lastEntry->slot_num;
					int lastEntryLength = lastEntry->slot_length;
					int needMovedEntryNum = lastEntryNum - deleteNextEntryNum + 1;
					// move forward data first. use another temp buffer to move
					void* anotherTempBuffer = calloc(1, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					memcpy(anotherTempBuffer, (char*)tempBuffer + deleteNextEntryOffset, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					memcpy((char*)tempBuffer + deleteEntryOffset, anotherTempBuffer, lastEntryOffset - deleteNextEntryOffset + lastEntryLength);
					free(anotherTempBuffer);
					//change rids
					for (int slotNum = deleteNextEntryNum; slotNum <= lastEntryNum; slotNum++) {
						int slotEntryAddr = PAGE_SIZE - FREE_ENTRY_OFFSET - sizeof(recordDirectoryEntry)* (slotNum + 1);
						((recordDirectoryEntry*)((char*)tempBuffer + slotEntryAddr))->slot_num--;
						((recordDirectoryEntry*)((char*)tempBuffer + slotEntryAddr))->slot_offset -= deleteEntryLength;
					}
					// move back rids backwards. use another temp buffer to move
					anotherTempBuffer = calloc(1, needMovedEntryNum * sizeof(recordDirectoryEntry));
					memcpy(anotherTempBuffer, (char*)tempBuffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* totalSlotNum, needMovedEntryNum * sizeof(recordDirectoryEntry));
					memcpy((char*)tempBuffer + freeOffsetaddr - sizeof(recordDirectoryEntry)* (totalSlotNum - 1), anotherTempBuffer, needMovedEntryNum * sizeof(recordDirectoryEntry));
					free(anotherTempBuffer);
					// change other parameters
					int * pToSlotNum = (int*)((char*)tempBuffer + slotNumaddr);
					*pToSlotNum = *pToSlotNum - 1;
					int * pToFreeOffset = (int*)((char*)tempBuffer + freeOffsetaddr);
					*pToFreeOffset = *pToFreeOffset - deleteEntryLength;
					int * pToFreeSpace = (int*)((char*)tempBuffer + freeSpaceaddr);
					*pToFreeSpace = *pToFreeSpace + deleteEntryLength + sizeof(recordDirectoryEntry);
					fileHandle.writePage(lastPageNum, tempBuffer);
					free(tempBuffer);
				}
			}
		}
	}

	return 0;
}*/
RC IndexManager::IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{	if (fileHandle.p == NULL) {
	return -1;
}
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int firstPageNum = fileHandle.getFirstPageNumber();
	int totalPageNum = fileHandle.getNumberOfPages();

	vector<int> trace;
	RID* idbuf;
	RecordBasedFileManager* rbfm = RecordBasedFileManager::instance();
	rbfm->openFile(fileHandle.name,ix_ScanIterator.handle);
	ix_ScanIterator.list.clear();
	ix_ScanIterator.next = 0;
	ix_ScanIterator.attrType.length = attribute.length;
	ix_ScanIterator.attrType.name = attribute.name;
	ix_ScanIterator.attrType.type = attribute.type;
	if (totalPageNum == 0)
	{
		return 0;
	}
	AttrType type = attribute.type;
	void* buffer = calloc(1, PAGE_SIZE);
	fileHandle.readPage(firstPageNum, buffer);
	trace.push_back(firstPageNum);
	int flag = *(int*)((char*)buffer + fullFlagaddr);
	int nextPageNum = -1;
	//find the start page of desired data based on lowkey
	cout<<"initialization success"<<endl;
	while (flag != -1)
	{
		int totalSlotNum = *(int*)((char*)buffer + slotNumaddr);
		//use lowKey to locate the start of the desired data
		if(lowKey == NULL)
		{
			nextPageNum = *(int*)((char*)buffer + fullFlagaddr);
		}
		else
		{
			int position = findInsertPosition(buffer, type, totalSlotNum, lowKey);

		//the comparison in findinsertposition should be no smaller than
			if (position == -1)
			{
				nextPageNum = *(int*)((char*)buffer + fullFlagaddr);
			}

				// else, we read the record on the position
			else
			{
				recordDirectoryEntry* entryOfIndexRecord = findEntry(buffer, position);
				int indexRecordOffset = entryOfIndexRecord->slot_offset;
				int indexRecordLength = entryOfIndexRecord->slot_length;
				idbuf = (RID*)((char*)buffer + indexRecordOffset + indexRecordLength - sizeof(RID));
				nextPageNum = idbuf->pageNum;
			}
		}
		if(nextPageNum == -1)
		{
			cout<<"scan error"<<endl;
		}
		//read the next page on the insert trace
		fileHandle.readPage(nextPageNum, buffer);
		trace.push_back(nextPageNum);
		flag = *(int*)((char*)buffer + fullFlagaddr);
	}
	cout<<"find page success"<<endl;
	int startPage = trace.back();
	//stopflag is used to indicate whether the loop of looking for desired data should be terminated
	// =1, go on, otherwise, terminate
	bool stopflag = 1;
	int lowint;
	int highint;
	float lowfloat;
	float highfloat;
	string lowstr;
	string highstr;
	int keyint;
	float keyfloat;
	string keystr;
	RID candidate;
	int strlen;
	//judge lowkey constraint
	bool iscandidate1 = 0;
	//judge highkey constraint
	bool iscandidate2 = 0;
	//find the candidate from startpage, if a entry satisfies lowkey constraint and highkey constraint simultaneously, it is
	//added to the list vector of ix iterator
	while(startPage != -1)
	{
		//cout<<"search in page "<<startPage<<endl;
		fileHandle.readPage(startPage,buffer);
		int totalSlotNum = *(int*)((char*)buffer + slotNumaddr);
		//cout<<"totalSlotNum is "<<totalSlotNum<<endl;
		for(int i = 0; i < totalSlotNum; i++)
		{
			//cout<<"check "<<i<<" th record in "<<startPage<<" page"<<endl;
			recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*)buffer + freeOffsetaddr - (i+1) * sizeof(recordDirectoryEntry));
			int entryoffset = rde->slot_offset;
			int entrylength = rde->slot_length;
			if(entrylength == -1)
			{
				continue;
			}
			//cout<<"the offset of "<<i<<" th record is "<<rde->slot_offset<<endl;
			//cout<<"the length of "<<i<<" th record is "<<rde->slot_length<<endl;
			iscandidate1 = 0;
			iscandidate2 = 0;
			candidate.pageNum = startPage;
			candidate.slotNum = i;
			switch(attribute.type)
			{
			case TypeInt:
				keyint = *((int*)((char*)buffer + entryoffset));
				if(lowKey == NULL)
				{
					iscandidate1 = 1;

				}
				else
				{
					lowint = *((int*)lowKey);
					if(lowint < keyint)
					{
						iscandidate1 = 1;
					}
					else if(lowint == keyint && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					highint = *((int*)highKey);
					if(keyint < highint)
					{
						iscandidate2 = 1;
					}
					else if(highint == keyint && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						//when encounter a key which is larger than highkey, it means that the search should be terminated
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				//cout<<"compare success"<<endl;
				break;
			case TypeReal:
				keyfloat = *((float*)((char*)buffer + entryoffset));
				if(lowKey == NULL)
				{
					iscandidate1 = 1;
				}
				else
				{
					lowfloat = *((float*)lowKey);
					if(lowfloat < keyfloat)
					{
						iscandidate1 = 1;
					}
					else if(lowfloat == keyfloat && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					highfloat = *((float*)highKey);
					if(keyfloat < highfloat)
					{
						iscandidate2 = 1;
					}
					else if(highfloat == keyfloat && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				break;
			case TypeVarChar:
				strlen = *((int*)((char*)buffer + entryoffset));
				keystr =  string((char*)buffer + entryoffset + sizeof(int), strlen);
				if(lowKey == NULL)
				{
					iscandidate1 = 1;
				}
				else
				{
					strlen = *((int*)lowKey);
					lowstr = string((char*)lowKey + sizeof(int), strlen);
					if(lowstr < keystr)
					{
						iscandidate1 = 1;
					}
					else if(lowstr == keystr && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					strlen = *((int*)highKey);
					highstr = string((char*)highKey + sizeof(int), strlen);
					if(keystr < highstr)
					{
						iscandidate2 = 1;
					}
					else if(highstr == keystr && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				break;
			default:
				break;
			}
			if(stopflag == 0)
			{
				break;
			}
			else
			{
				if(iscandidate1 == 1 && iscandidate2 == 1)
				{
					ix_ScanIterator.list.push_back(candidate);
					//cout<<"push the record information to list success"<<endl;
				}

			}
			//cout<<"loop continue"<<endl;
		}
		if(stopflag == 0)
		{
			break;
		}
		int* pToNextPage = (int*)((char*)buffer + nextPageaddr);
		startPage = *pToNextPage;
	}
	return 0;
}
/*RC IndexManager::IndexManager::scan(FileHandle &fileHandle,
    const Attribute &attribute,
    const void      *lowKey,
    const void      *highKey,
    bool			lowKeyInclusive,
    bool        	highKeyInclusive,
    IX_ScanIterator &ix_ScanIterator)
{	if (fileHandle.p == NULL) {
	return -1;
}
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int firstPageNum = fileHandle.getFirstPageNumber();
	int totalPageNum = fileHandle.getNumberOfPages();

	vector<int> trace;
	RID* idbuf;
	ix_ScanIterator.handle.p = fileHandle.p;
	ix_ScanIterator.list.clear();
	ix_ScanIterator.next = 0;
	ix_ScanIterator.attrType.length = attribute.length;
	ix_ScanIterator.attrType.name = attribute.name;
	ix_ScanIterator.attrType.type = attribute.type;
	if (totalPageNum == 0)
	{
		return 0;
	}
	AttrType type = attribute.type;
	void* buffer = calloc(1, PAGE_SIZE);
	fileHandle.readPage(firstPageNum, buffer);
	trace.push_back(firstPageNum);
	int flag = *(int*)((char*)buffer + fullFlagaddr);
	int nextPageNum = -1;
	//find the start page of desired data based on lowkey
	//cout<<"initialization success"<<endl;
	while (flag != -1)
	{
		int totalSlotNum = *(int*)((char*)buffer + slotNumaddr);
		//use lowKey to locate the start of the desired data
		if(lowKey == NULL)
		{
			nextPageNum = *(int*)((char*)buffer + fullFlagaddr);
		}
		else
		{
			int position = findInsertPosition(buffer, type, totalSlotNum, lowKey);

		//the comparison in findinsertposition should be no smaller than
			if (position == -1)
			{
				nextPageNum = *(int*)((char*)buffer + fullFlagaddr);
			}

				// else, we read the record on the position
			else
			{
				recordDirectoryEntry* entryOfIndexRecord = findEntry(buffer, position);
				int indexRecordOffset = entryOfIndexRecord->slot_offset;
				int indexRecordLength = entryOfIndexRecord->slot_length;
				idbuf = (RID*)((char*)buffer + indexRecordOffset + indexRecordLength - sizeof(RID));
				nextPageNum = idbuf->pageNum;
			}
		}
		if(nextPageNum == -1)
		{
			cout<<"scan error"<<endl;
		}
		//read the next page on the insert trace
		fileHandle.readPage(nextPageNum, buffer);
		trace.push_back(nextPageNum);
		flag = *(int*)((char*)buffer + fullFlagaddr);
	}
	//cout<<"find page success"<<endl;
	int startPage = trace.back();
	//stopflag is used to indicate whether the loop of looking for desired data should be terminated
	// =1, go on, otherwise, terminate
	bool stopflag = 1;
	int lowint;
	int highint;
	float lowfloat;
	float highfloat;
	string lowstr;
	string highstr;
	int keyint;
	float keyfloat;
	string keystr;
	RID candidate;
	int strlen;
	//judge lowkey constraint
	bool iscandidate1 = 0;
	//judge highkey constraint
	bool iscandidate2 = 0;
	RID *id;
	//find the candidate from startpage, if a entry satisfies lowkey constraint and highkey constraint simultaneously, it is
	//added to the list vector of ix iterator
	while(startPage != -1)
	{
		//cout<<"search in page "<<startPage<<endl;
		fileHandle.readPage(startPage,buffer);
		int totalSlotNum = *(int*)((char*)buffer + slotNumaddr);
		//cout<<"totalSlotNum is "<<totalSlotNum<<endl;
		for(int i = 0; i < totalSlotNum; i++)
		{
			//cout<<"check "<<i<<" th record in "<<startPage<<" page"<<endl;
			recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*)buffer + freeOffsetaddr - (i+1) * sizeof(recordDirectoryEntry));
			int entryoffset = rde->slot_offset;
			int entrylength = rde->slot_length;

			//cout<<"the offset of "<<i<<" th record is "<<rde->slot_offset<<endl;
			//cout<<"the length of "<<i<<" th record is "<<rde->slot_length<<endl;
			iscandidate1 = 0;
			iscandidate2 = 0;
			//candidate.pageNum = startPage;
			//candidate.slotNum = i;
			switch(attribute.type)
			{
			case TypeInt:
				id = (RID*)((char*)buffer + entryoffset + sizeof(int));
				candidate.pageNum = id->pageNum;
				candidate.slotNum = id->slotNum;
				keyint = *((int*)((char*)buffer + entryoffset));
				if(lowKey == NULL)
				{
					iscandidate1 = 1;

				}
				else
				{
					lowint = *((int*)lowKey);
					if(lowint < keyint)
					{
						iscandidate1 = 1;
					}
					else if(lowint == keyint && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					highint = *((int*)highKey);
					if(keyint < highint)
					{
						iscandidate2 = 1;
					}
					else if(highint == keyint && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						//when encounter a key which is larger than highkey, it means that the search should be terminated
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				if(stopflag == 0)
				{
					break;
				}
				else
				{
					if(iscandidate1 == 1 && iscandidate2 == 1)
					{
						ix_ScanIterator.listint.push_back(keyint);
						ix_ScanIterator.list.push_back(candidate);
									//cout<<"push the record information to list success"<<endl;
					}
				}
				//cout<<"compare success"<<endl;
				break;
			case TypeReal:
				keyfloat = *((float*)((char*)buffer + entryoffset));
				id = (RID*)((char*)buffer + entryoffset + sizeof(float));
				candidate.pageNum = id->pageNum;
				candidate.slotNum = id->slotNum;
				if(lowKey == NULL)
				{
					iscandidate1 = 1;
				}
				else
				{
					lowfloat = *((float*)lowKey);
					if(lowfloat < keyfloat)
					{
						iscandidate1 = 1;
					}
					else if(lowfloat == keyfloat && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					highfloat = *((float*)highKey);
					if(keyfloat < highfloat)
					{
						iscandidate2 = 1;
					}
					else if(highfloat == keyfloat && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				if(stopflag == 0)
				{
					break;
				}
				else
				{
					if(iscandidate1 == 1 && iscandidate2 == 1)
					{
						ix_ScanIterator.listfloat.push_back(keyfloat);
						ix_ScanIterator.list.push_back(candidate);
						//cout<<"push the record information to list success"<<endl;
					}
				}
				break;
			case TypeVarChar:
				strlen = *((int*)((char*)buffer + entryoffset));
				keystr =  string((char*)buffer + entryoffset + sizeof(int), strlen);
				id = (RID*)((char*)buffer + entryoffset + sizeof(int) + strlen);
				candidate.pageNum = id->pageNum;
				candidate.slotNum = id->slotNum;
				if(lowKey == NULL)
				{
					iscandidate1 = 1;
				}
				else
				{
					strlen = *((int*)lowKey);
					lowstr = string((char*)lowKey + sizeof(int), strlen);
					if(lowstr < keystr)
					{
						iscandidate1 = 1;
					}
					else if(lowstr == keystr && lowKeyInclusive == 1)
					{
						iscandidate1 = 1;
					}
					else
					{
						iscandidate1 = 0;
					}
				}
				if(highKey == NULL)
				{
					iscandidate2 = 1;
				}
				else
				{
					strlen = *((int*)highKey);
					highstr = string((char*)highKey + sizeof(int), strlen);
					if(keystr < highstr)
					{
						iscandidate2 = 1;
					}
					else if(highstr == keystr && highKeyInclusive == 1)
					{
						iscandidate2 = 1;
					}
					else
					{
						stopflag = 0;
						iscandidate2 = 0;
					}
				}
				if(stopflag == 0)
				{
					break;
				}
				else
				{
					if(iscandidate1 == 1 && iscandidate2 == 1)
					{
						ix_ScanIterator.liststring.push_back(keystr);
						ix_ScanIterator.list.push_back(candidate);
												//cout<<"push the record information to list success"<<endl;
					}
				}
				break;
			default:
				break;
			}
			//cout<<"loop continue"<<endl;
		}
		if(stopflag == 0)
		{
			break;
		}
		int* pToNextPage = (int*)((char*)buffer + nextPageaddr);
		startPage = *pToNextPage;
	}
	return 0;
}*/

IX_ScanIterator::IX_ScanIterator()
{
	handle.p = NULL;
	list.clear();
	next = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
    if(handle.p != NULL)
    {
   	 PagedFileManager* im = PagedFileManager::instance();
   	 im->closeFile(handle);
    }
	 list.clear();
	 listint.clear();
	 listfloat.clear();
	 liststring.clear();
	 next = 0;
}

/*RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	//void* buffer = malloc(PAGE_SIZE);
	RID nextid;
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int keylen;
	string str;
	//RID* id;
	if(next < list.size())
	{
		//nextid.pageNum = list[next].pageNum;
		//nextid.slotNum = list[next].slotNum;
		//handle.readPage(nextid.pageNum,buffer);

		//recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*)buffer + freeOffsetaddr - (nextid.slotNum + 1) * sizeof(recordDirectoryEntry));
		//int offset = rde->slot_offset;
		//int length = rde->slot_length;
		switch(attrType.type)
		{
		case TypeInt:
			*((int*)key) = listint[next];
			break;
		case TypeReal:
			*((float*)key) = listfloat[next];
			break;
		case TypeVarChar:
			str = liststring[next];
			keylen = str.size();
			memcpy(key,&keylen,sizeof(int));
			memcpy((char*)key + sizeof(int), str.c_str(), keylen);
			break;
		default:
			break;

		}
		//memcpy(key, (char*)buffer + offset, keylen);
		//id = (RID*)((char*)buffer + offset + keylen);
		rid.pageNum = list[next].pageNum;
		rid.slotNum = list[next].slotNum;
		next++;
	 	//free(buffer);
	 	return 0;
	}
	//free(buffer);
	return IX_EOF;
}*/
RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	void* buffer = malloc(PAGE_SIZE);
	RID nextid;
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	int keylen;
	RID* id;
	if(next < list.size())
	{
		nextid.pageNum = list[next].pageNum;
		nextid.slotNum = list[next].slotNum;
		handle.readPage(nextid.pageNum,buffer);
		recordDirectoryEntry* rde = (recordDirectoryEntry*) ((char*)buffer + freeOffsetaddr - (nextid.slotNum + 1) * sizeof(recordDirectoryEntry));
		int offset = rde->slot_offset;
		int length = rde->slot_length;
		switch(attrType.type)
		{
		case TypeInt:
			keylen = sizeof(int);
			break;
		case TypeReal:
			keylen = sizeof(float);
			break;
		case TypeVarChar:
			keylen = sizeof(int) + *((int*)((char*)buffer + offset));
			break;
		default:
			break;

		}
		memcpy(key, (char*)buffer + offset, keylen);
		id = (RID*)((char*)buffer + offset + keylen);
		rid.pageNum = id->pageNum;
		rid.slotNum = id->slotNum;
		next++;
	 	free(buffer);
	 	return 0;
	}
	free(buffer);
	return IX_EOF;
}


RC IX_ScanIterator::close()
{
	 PagedFileManager* im = PagedFileManager::instance();
	 im->closeFile(handle);
	 //handle.p = NULL;
	 list.clear();
	// cout<<"in iterator close, size of list is "<<list.size()<<endl;
	 listint.clear();
	 listfloat.clear();
	 liststring.clear();
	 next = 0;
	 return 0;

}

void IX_PrintError (RC rc)
{
}
