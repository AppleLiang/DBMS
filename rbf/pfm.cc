#include "pfm.h"
#include "stdlib.h"
#include <string.h>
#include <string>
#include <stdio.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const char *fileName)
{
	//cout<<"enter create file"<<endl;
	//check whether fileName is valid
	if(fileName == NULL || strlen(fileName) == 0)
	{
		return -1;
	}

	//check whether file already exists on the disk
	FILE* fp;
	if(FileExists(fileName))
		return -1;

    //create file
	if((fp = fopen(fileName,"w+b")) != NULL)
	//successfully create a file, set the pagenum to zero
	{
		PageNum pagenum = 0;
		fseek(fp, HEAD_PAGENUM_OFFSET, SEEK_SET);
		fwrite(&pagenum,sizeof(PageNum),1,fp);
		fflush(fp);

		int empty = -1;
		fseek(fp, HEAD_EMPTY_OFFSET, SEEK_SET);
		fwrite(&empty,sizeof(int),1,fp);
		fflush(fp);

		int full = -1;
		fseek(fp, HEAD_FULL_OFFSET, SEEK_SET);
		fwrite(&full,sizeof(int),1,fp);

		counter[fileName] = 0;
		fclose(fp);
		return 0;
	}
	//if the file is not successfully created, delete the file from fireDirectory
	//fileDirectory.erase(str);
	return -1;
}


RC PagedFileManager::destroyFile(const char *fileName)
{
	//cout<<"enter destory file"<<endl;
	if(counter.find(fileName) != counter.end())
	{
		if(counter[fileName] == 0)
		{
			return remove(fileName);
		}
		else
		{
			cout<<counter[fileName]<<endl;
			cout<<"file is in use, can not destroy it now"<<endl;
		}
	}
	return -1;
}

bool PagedFileManager::FileExists(const char *fileName)
{
	struct stat stFileInfo;

	if(stat(fileName,&stFileInfo) == 0) return true;
	else return false;

}

RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	if(fileName == NULL || strlen(fileName) == 0)
	{
		return -1;
	}


	if(fileHandle.p != NULL)
	{
		return -1;
	}

	if(!FileExists(fileName))
		return -1;

	if((fileHandle.p = fopen(fileName,"r+b")) != NULL)
	{
		if(counter.find(fileName) == counter.end())
		{
			counter[fileName] = 0;
		}
		counter[fileName] ++;
		fileHandle.name = fileName;
		//cout<<"file"<<fileHandle.name<<" has "<<counter[fileName]<<" pointers"<<endl;
		return 0;
	}
	return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (fileHandle.p == NULL)
	{
		return 0;
	}
	if (!fclose(fileHandle.p))
	{

		counter[fileHandle.name]--;
		fileHandle.name.clear();
		fileHandle.p = NULL;
		return 0;
	}
	return -1;

}

FileHandle::FileHandle()
{
	p = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(p == NULL)
	{
		return -1;
	}
	PageNum pnum = getNumberOfPages();
	if( pnum <= pageNum )
	{
		return -1;
	}

	fseek(p, PAGE_START_ADDRESS + PAGE_SIZE * pageNum, SEEK_SET);
	size_t success = fread(data ,PAGE_SIZE , 1, p);

	if( success != 1)
	{
		return -1;
	}

	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(p == NULL )
	{
		return -1;
	}

	PageNum pnum = getNumberOfPages();
	if(pnum <= pageNum)
	{
		return -1;
	}

	fseek(p, PAGE_START_ADDRESS + PAGE_SIZE * pageNum, SEEK_SET);
	size_t success = fwrite(data,PAGE_SIZE,1,p);
	fflush(p);
	if(success != 1)
	{
		return -1;
	}
	return 0;

}


RC FileHandle::appendPage(const void *data)
{
	if(p == NULL)
	{
		return -1;
	}

	PageNum pagenum = getNumberOfPages() + 1;
	fseek(p,HEAD_PAGENUM_OFFSET,SEEK_SET);
	size_t success = fwrite(&pagenum,sizeof(PageNum),1,p);
	fflush(p);
	if(success != 1)
	{
		return -1;
	}
	--pagenum;
	return writePage(pagenum,data);
}

RC FileHandle::removePageFromList(PageNum pageNum, ListType listType)
{
	int page = (int)pageNum;
	int previousPage;
	int nextPage;
	size_t success;
	RC rc;
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	void* buffer = malloc(PAGE_SIZE);
	rc = readPage(pageNum,buffer);
	if(rc != 0)
	{
		cout<<"fail at read page step"<<endl;
		return -1;
	}
	previousPage = *((int*) ((char*)buffer + prevPageaddr));
	nextPage = *((int*) ((char*)buffer + nextPageaddr));

	if(nextPage != -1)
	{
		rc = readPage(nextPage, buffer);
		if(rc != 0)
		{
			cout<<"fail at read next page step"<<endl;
			return -1;
		}
		*((int*) ((char*)buffer + prevPageaddr))=previousPage;
		//*prevEntry = previousPage;
		rc = writePage(nextPage, buffer);
		if(rc != 0)
		{
			cout<<"fail at write next page step"<<endl;
			return -1;
		}
	}

	//if(pageNum == 0)
	//{
		//cout<<pageNum<<" is removed from emptylist"<<endl;
		//cout<<"its previous page is "<<previousPage<<endl;
		//cout<<"its next page is "<<nextPage<<endl;
	//}
	if(previousPage != -1)
		//change the next pointer of previous page
	{
		rc = readPage(previousPage, buffer);
		if(rc != 0)
		{
			cout<<"fail at read previous page step"<<endl;
			return -1;
		}
		*((int*) ((char*)buffer + nextPageaddr)) = nextPage;
		//*nextEntry = nextPage;
		rc = writePage(previousPage, buffer);
		if(rc != 0)
		{
			cout<<"fail at write previous page step"<<endl;
			return -1;
		}
	}
	else
	{
		switch(listType)
		{
		case EmptyList:
			fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
			if(pageNum == 0)
			{
				//cout<<pageNum<<" is removed from emptylist"<<endl;
				//cout<<"its previous page is "<<previousPage<<endl;
				//cout<<"its next page is "<<nextPage<<endl;
			}
			break;
		case FullList:
		    fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
			if(pageNum == 0)
			{
				//cout<<pageNum<<" is removed from fulllist"<<endl;
				//cout<<"its previous page is "<<previousPage<<endl;
				//cout<<"its next page is "<<nextPage<<endl;
			}
		    break;
		default:
			return -1;
			break;
		}
		success = fwrite(&nextPage, sizeof(int), 1, p);
		fflush(p);
		if (success != 1)
		{
			cout<<"fail at write header page step"<<endl;
			return -1;
		}
	}
	free(buffer);
	return 0;
}
/*RC FileHandle::removePageFromList(PageNum pageNum, ListType listType)
{
	int page = (int)pageNum;
	int previousPage;
	int nextPage;
	size_t success;
	//get the next page of the page to be deleted
	fseek(p, PAGE_START_ADDRESS + pageNum * PAGE_SIZE + PAGE_SIZE - NEXT_ENTRY_OFFSET, SEEK_SET);
	fread(&nextPage, sizeof(int), 1, p);
	//get the previous page of the page to be deleted
	fseek(p, PAGE_START_ADDRESS + pageNum * PAGE_SIZE + PAGE_SIZE - PREVIOUS_ENTRY_OFFSET, SEEK_SET);
	fread(&previousPage, sizeof(int), 1, p);

	if(nextPage != -1)
	//change the previous pointer of next page, if exist
	{
		fseek(p, PAGE_START_ADDRESS + nextPage * PAGE_SIZE + PAGE_SIZE - PREVIOUS_ENTRY_OFFSET, SEEK_SET);
		success = fwrite(&previousPage, sizeof(int), 1, p);
		fflush(p);
		if (success != 1)
		{
			return -1;
		}
	}

	if(previousPage != -1)
	//change the next pointer of previous page
	{
		fseek(p, PAGE_START_ADDRESS + previousPage * PAGE_SIZE + PAGE_SIZE - NEXT_ENTRY_OFFSET, SEEK_SET);
		success = fwrite(&nextPage, sizeof(int), 1, p);
	}
	else
	{
		switch(listType)
		{
		case EmptyList:
			fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
			break;
		case FullList:
			fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
			break;
		default:
			return -1;
			break;
		}
		success = fwrite(&nextPage, sizeof(int), 1, p);
	}
	fflush(p);
	if (success != 1)
	{
		return -1;
	}
	return 0;
}*/


/*RC FileHandle::addPageToList(PageNum pageNum, ListType listType)
{
	//insert a page between the head pointer and the firstpage of the list
	int page = (int)pageNum;
	int firstPage;
	int previousPage = -1;
	size_t success;
	switch(listType)
	{
	case EmptyList:
		//get the firstpage
		fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
		fread(&firstPage, sizeof(int), 1, p);
		//change the head pointer
		fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
		success = fwrite(&page, sizeof(int), 1, p);
		break;
	case FullList:
		//get the firstpage
		fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
		fread(&firstPage, sizeof(int), 1, p);
		//change the head pointer
		fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
		success = fwrite(&page, sizeof(int), 1, p);
		break;
	default:
		return -1;
		break;
	}
	fflush(p);
	if (success != 1)
	{
		return -1;
	}
	//change the previous pointer of firstpage, if exists
	if(firstPage != -1)
	{
		fseek(p, PAGE_START_ADDRESS + firstPage * PAGE_SIZE + PAGE_SIZE - PREVIOUS_ENTRY_OFFSET, SEEK_SET);
		success = fwrite(&page, sizeof(int), 1, p);
		fflush(p);
		if (success != 1)
		{
			return -1;
		}
	}
	//change the next pointer of the inserted page
	fseek(p, PAGE_START_ADDRESS + pageNum * PAGE_SIZE + PAGE_SIZE - NEXT_ENTRY_OFFSET, SEEK_SET);
	success = fwrite(&firstPage, sizeof(int), 1, p);
	fflush(p);
	if (success != 1)
	{
		return -1;
	}
	//change the previous pointer of the inserted page, definitely -1
	fseek(p, PAGE_START_ADDRESS + pageNum * PAGE_SIZE + PAGE_SIZE - PREVIOUS_ENTRY_OFFSET, SEEK_SET);
	success = fwrite(&previousPage, sizeof(int), 1, p);
	fflush(p);
	if (success != 1)
	{
		return -1;
	}
	return 0;
}*/
RC FileHandle::addPageToList(PageNum pageNum, ListType listType)
{
	int page = (int)pageNum;
	int previousPage = -1;
	int nextPage;
	int firstPage;
	size_t success;
	RC rc;
	int freeOffsetaddr = PAGE_SIZE - FREE_ENTRY_OFFSET;
	int freeSpaceaddr = PAGE_SIZE - FREE_SPACE_OFFSET;
	int slotNumaddr = PAGE_SIZE - SLOTNUM_ENTRY_OFFSET;
	int prevPageaddr = PAGE_SIZE - PREVIOUS_ENTRY_OFFSET;
	int nextPageaddr = PAGE_SIZE - NEXT_ENTRY_OFFSET;
	int fullFlagaddr = PAGE_SIZE - FULL_FLAG;
	void* buffer = malloc(PAGE_SIZE);

	switch(listType)
	{
	case EmptyList:
		//get the firstpage
		fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
		fread(&firstPage, sizeof(int), 1, p);
		//change the head pointer
		fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
		success = fwrite(&page, sizeof(int), 1, p);
		break;
	case FullList:
		//get the firstpage
		fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
		fread(&firstPage, sizeof(int), 1, p);
		//change the head pointer
		fseek(p, HEAD_FULL_OFFSET, SEEK_SET);
		success = fwrite(&page, sizeof(int), 1, p);
		break;
	default:
		return -1;
		break;
	}
	fflush(p);

	if (success != 1)
	{
		return -1;
	}


		//cout<<pageNum<<" is added to fulllist"<<endl;
		//cout<<"its next page is "<<firstPage<<endl;
		//cout<<"its previous page is "<<previousPage<<endl;
	if(firstPage != -1)
	{
		rc = readPage(firstPage, buffer);
		if(rc != 0)
		{
			return -1;
		}
		*(int*) ((char*)buffer + prevPageaddr) = page;
		//*prevEntry = page;
		rc = writePage(firstPage, buffer);
		if(rc != 0)
		{
			return -1;
		}
	}

	rc = readPage(pageNum, buffer);
	if(rc != 0)
	{
		return -1;
	}
	*((int*) ((char*)buffer + nextPageaddr)) = firstPage;
	//*nextEntry = firstPage;
	*((int*) ((char*)buffer + prevPageaddr)) = previousPage;
	//*prevEntry = previousPage;
	//int* fullFlag = (int*) ((char*)buffer + fullFlagaddr);
	switch(listType)
	{
	case EmptyList:
		*((int*) ((char*)buffer + fullFlagaddr)) = 0;
		 //cout<<page<<"is added to emptylist"<<endl;
		 //cout<<"its previous page is "<<previousPage<<endl;
		 //cout<<"its next page is "<<firstPage<<endl;
		break;
	case FullList:
		*((int*) ((char*)buffer + fullFlagaddr)) = 1;
		//cout<<page<<"is added to fulllist"<<endl;
		//cout<<"its previous page is "<<previousPage<<endl;
		//cout<<"its next page is "<<firstPage<<endl;
		break;
	}
	rc = writePage(pageNum, buffer);
	free(buffer);
	return rc;
}

// getters and settes
unsigned FileHandle::getNumberOfPages() {
	PageNum pageNum;
	fseek(p,HEAD_PAGENUM_OFFSET,SEEK_SET);
	fread(&pageNum,sizeof(PageNum),1,p);
    return pageNum;
}
void FileHandle::setNumberOfPages(int pageNum) {
	fseek(p, HEAD_PAGENUM_OFFSET, SEEK_SET);
	fwrite(&pageNum, sizeof(PageNum), 1, p);
}
unsigned FileHandle::getFirstPageNumber() {
	PageNum firstPageNum;
	fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
	fread(&firstPageNum, sizeof(PageNum), 1, p);
	return firstPageNum;
}
void FileHandle::setFirstPageNumber(int firstPageNum) {
	fseek(p, HEAD_EMPTY_OFFSET, SEEK_SET);
	fwrite(&firstPageNum, sizeof(PageNum), 1, p);
}

