use std::collections::BTreeMap;
use std::io;
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use crate::fileheader::FileHeader;
use crate::freelist::FreeList;
use crate::slot::Slot;
use serde::{Serialize, Deserialize};
use crate::persist::KVError::KeyDoesNotExist;

#[derive(Debug)]
enum KVError {
    KeyDoesNotExist,
    IOError(std::io::Error),
}

pub struct Persister<K> {
    freelist: FreeList,  
    header: FileHeader,
    index: BTreeMap<K, Slot>, // todo(): unify SlotInstance with a more common name
    last_cursor: usize,
}

impl<K> Persister<K> where K: Ord {
    pub fn new(datastore: String, storage_limit: usize) -> Result<Self, KVError> {
        FileHeader::new(Some(datastore))
            .map(|fh| Self { freelist: FreeList::new(), header: fh, index: BTreeMap::new(), last_cursor: 0 })
            .map_err(|io_error| KVError::IOError(io_error))
    }

    pub fn insert_kv<'a>(&mut self, key: K, value: &Vec<u8>) -> Result<usize, std::io::Error>
    where K: Serialize + Deserialize<'a> {
        let mut cursor: usize;

        // todo(): handle the case when the key has already been inserted

        // try to retrieve free space, otherwise, add in the last cursor
        match self.freelist.retrieve_free_space(value.len()) {
            Some(empty_space_cursor) => cursor = empty_space_cursor,
            None => {
                cursor = self.last_cursor;
                self.last_cursor = self.last_cursor + value.len();
            }
        }

        if let Err(error) = self.persist_value(&value, cursor) {
            // make sure to free the memory to prevent leaks
            if cursor == self.last_cursor - value.len() {
                self.last_cursor = cursor - value.len()
            }
            return Err(error)
        }

        // todo(): serialize and store the key in file

        // insert key in index
        if self.index.insert(key, Slot {cursor, space: value.len()}).is_none() {
            // todo(): return error and undo things
        }

        return Ok(cursor);
    }

    pub fn get_value(&mut self, key: &K) -> Result<Vec<u8>, std::io::Error> {
        if !self.index.contains_key(key) {
            return Err(Error::new(ErrorKind::Other, "The key introduced was not registered"));
        }

        // retrieve value from mem
        match self.index.get(key) {
            Some(val) => {
                return self.retrieve_value(val.cursor, val.space);
            },
            None => {
                return Err(Error::new(ErrorKind::Other, "Unexpected error retrieving key from index"));
            }
        }
    }

    pub fn update_kv(&mut self, key: &K, value: &Vec<u8>) -> Result<(), KVError> {
        let mut slot = Slot{space: 0, cursor: 0};
        match self.index.get(key) {
            Some(val) => {
                slot = val.clone();
            },
            None => return Err(KeyDoesNotExist),
        }

        // free previous data and claim more space
        if value.len() > slot.space {
            self.freelist.insert_free_space(slot.cursor, slot.space);
            match self.freelist.retrieve_free_space(slot.space) {
                Some(val) => {
                    slot.cursor = val;
                },
                None => {
                    slot.cursor = self.last_cursor;
                    self.last_cursor = self.last_cursor + value.len();
                },
            }
        }

        // downsize the leftover space if the space is smaller
        if value.len() < slot.space {
            self.freelist.insert_free_space(slot.cursor+value.len(), slot.space - value.len());
        }

        // update slot space required
        slot.space = value.len();

        // persist the value
        self.persist_value(value, slot.cursor);

        // todo(): serialize the new key data

        // update the index
        self.index.insert(key, Slot{cursor: slot.cursor, space: slot.space});

        return Ok(())
    }

    pub fn delete_kv(&mut self, key: &K) -> Result<(), KVError> {
        // check if key exists and insert freed space
        match self.index.get(key) {
            Some(val) => self.freelist.insert_free_space(val.cursor, val.space),
            None => return Err(KeyDoesNotExist),
        }

        // todo(): remove serialized key from file

        // remove key from index
        match self.index.remove(key) {
            Some(_) => Ok(()),
            None => Err(KeyDoesNotExist), // should never happen
        }
    }

    fn persist_value(&mut self, data: &Vec<u8>, cursor: usize) -> Result<(), std::io::Error> {
        self.header.db_file.seek(SeekFrom::Start(cursor as u64))?;
        self.header.db_file.write_all(data.as_ref())?;

        Ok(())
    }

    fn persist_key(&mut self) -> Result<(), KVError> {
        return Ok(());
    }

    fn retrieve_value(&mut self, cursor: usize, space: usize) -> Result<Vec<u8>, std::io::Error> {
        // todo(buffer): use a fixed buffer instead of a vec
        let mut buffer = Vec::with_capacity(space);

        // todo: handle the error and returns
        self.header.db_file.seek(SeekFrom::Start(cursor as u64));
        self.header.db_file.read_at(&mut buffer.as_mut_slice(), cursor as u64)?;

        return Ok(buffer)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_kv() {
        assert_eq!(1, 2)
    }

    #[test]
    fn test_get_value() {
        assert_eq!(1, 2)
    }

    #[test]
    fn test_update_value() {
        assert_eq!(1, 2)
    }

    #[test]
    fn delete_kv() {
        assert_eq!(1, 2)
    }
}
