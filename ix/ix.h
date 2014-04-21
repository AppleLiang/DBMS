
#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stdio.h>
#include <string.h>
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;

class IndexManager {
 public:
  static IndexManager* instance();

  RC createFile(const string &fileName);

  RC destroyFile(const string &fileName);

  RC openFile(const string &fileName, FileHandle &fileHandle);

  RC closeFile(FileHandle &fileHandle);

  // The following two functions are using the following format for the passed key value.
  //  1) data is a concatenation of values of the attributes
  //  2) For int and real: use 4 bytes to store the value;
  //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
  RC insertEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Insert new index entry
  RC deleteEntry(FileHandle &fileHandle, const Attribute &attribute, const void *key, const RID &rid);  // Delete index entry

  // scan() returns an iterator to allow the caller to go through the results
  // one by one in the range(lowKey, highKey).
  // For the format of "lowKey" and "highKey", please see insertEntry()
  // If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
  // should be included in the scan
  // If lowKey is null, then the range is -infinity to highKey
  // If highKey is null, then the range is lowKey to +infinity
  RC scan(FileHandle &fileHandle,
      const Attribute &attribute,
	  const void        *lowKey,
      const void        *highKey,
      bool        lowKeyInclusive,
      bool        highKeyInclusive,
      IX_ScanIterator &ix_ScanIterator);

  // This function is used to insert entry to certain page. trace is used to record our searching trace, which is a vertor of pageNum.
  // flag means whether this page is a leaf page or not. -1 leaf, non -1 non leaf.
  RC insertEntryToPage (FileHandle &fileHandle, const Attribute &attribute, const void *key,
		  const RID &rid, vector<int> &trace, int flag);

  // This function is used to find the position in insert and search. The data should be after the return value
  int findInsertPosition(void* buffer, AttrType type, int total, const void *key);

  // This function is used to find the position of the entry that need to be deleted
  int findDeletePosition(void* buffer, AttrType type, int total, const void *key);

  // This function is used to split a page. the 1st parameter is used to write, second-4th used to findinsert postion, 4th and 5th used
  // to insert. pageNum used to write, flag used to seperate issues and the left page num when it's not -1, trace used to track.
  RC splitPage(FileHandle &fileHandle, void* buffer, const Attribute &attribute, int total, const void *key,
  		  const RID &rid, int pageNum, vector<int> &trace, int flag);

  // This function is used to find the nth entry in certain page
  recordDirectoryEntry* findEntry(void* buffer, int entryNum);

  RC reorganizePage(FileHandle &fileHandle, vector<int>& trace);

  // This function is used to insert a tuple to a certain position in a page if this page could fit this record
  void insertToPosition(FileHandle &fileHandle, void* buffer, void* data, const int insertPosition, const int sizeOfInsertEntry, const int totalRecordNum, int pageNum);

 protected:
  IndexManager   ();                            // Constructor
  ~IndexManager  ();                            // Destructor

 private:
  static IndexManager *_index_manager;
};

class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan
  FileHandle handle;// Terminate index scan
  vector<RID> list;
  vector<int> listint;
  vector<float> listfloat;
  vector<string> liststring;
  Attribute attrType;
  int next;
};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
