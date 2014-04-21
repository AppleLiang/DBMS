#ifndef _pfm_h_
#define _pfm_h_

typedef int RC;
typedef unsigned PageNum;

#define PAGE_SIZE 4096
#define PAGE_START_ADDRESS sizeof(unsigned)+2*sizeof(int)
#define HEAD_PAGENUM_OFFSET 0
#define HEAD_EMPTY_OFFSET sizeof(unsigned)
#define HEAD_FULL_OFFSET sizeof(unsigned)+sizeof(int)
#define NEXT_ENTRY_OFFSET sizeof(int)
#define PREVIOUS_ENTRY_OFFSET 2*sizeof(int)
#define SLOTNUM_ENTRY_OFFSET 3*sizeof(int)
#define FULL_FLAG 4 * sizeof(int)
#define FREE_SPACE_OFFSET 5*sizeof(int)//add a free space offset
#define FREE_ENTRY_OFFSET 6*sizeof(int)


#define DELETED -1
#define TOMB_STONE 0

class FileHandle;

#include"stdio.h"
#include"stddef.h"
#include"sys/stat.h"
#include<iostream>
#include<string>
#include<map>
using std::map;
using std::string;
using std::pair;
using std::cerr;
using std::cout;
using std::endl;

typedef pair<map<string,int>::iterator, bool> InsertReturnType;
typedef pair<string,int> FileDirectEntry;
typedef enum { FullList, EmptyList } ListType;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                     // Access to the _pf_manager instance

    RC createFile    (const char *fileName);                         // Create a new file
    RC destroyFile   (const char *fileName);                         // Destroy a file
    RC openFile      (const char *fileName, FileHandle &fileHandle); // Open a file
    RC closeFile     (FileHandle &fileHandle);                       // Close a file
    bool FileExists  (const char* fileName);
    map<string, int> counter;
protected:
    PagedFileManager();                                   // Constructor
    ~PagedFileManager();                                  // Destructor

private:
    static PagedFileManager *_pf_manager;
    map<string,int> fileDirectory;

};


class FileHandle
{
public:
    FileHandle();                                                    // Default constructor
    ~FileHandle();                                                   // Destructor

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);
    RC addPageToList(PageNum pagenum, ListType listType);// Append a specific page
    RC removePageFromList(PageNum pagenum, ListType listType);
	// getters and settes
    unsigned getNumberOfPages();
	void setNumberOfPages(int pageNum);
    unsigned getFirstPageNumber();
	void setFirstPageNumber(int firstPageNum);
    FILE* p;// Get the number of pages in the file
    string name;
 };

 #endif
