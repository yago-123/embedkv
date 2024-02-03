use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use crate::fileheader::FileHeader;
use crate::freelist::FreeList;
use crate::slot::Slot;
use serde::{Serialize, Deserialize};

pub struct Persister<K> {
    freelist: FreeList,  
    header: FileHeader,
    index: BTreeMap<K, Slot>, // todo(): unify SlotInstance with a more common name
    last_cursor: usize,
}

impl<K> Persister<K> where K: Ord {
    pub fn new(datastore: String, storage_limit: usize) -> Result<Self, std::io::Error> {
        FileHeader::new(Some(datastore))
            .map(|fh| Self { freelist: FreeList::new(), header: fh, index: BTreeMap::new(), last_cursor: 0 })
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

        if let Err(error) = self.insert_value(&value, cursor) {
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

    pub fn update_kv(&mut self, key: K) {

    }

    pub fn delete_kv(&mut self, key: K) {

    }

    fn insert_value(&mut self, data: &Vec<u8>, cursor: usize) -> Result<(), std::io::Error> {
        self.header.db_file.seek(SeekFrom::Start(cursor as u64))?;
        self.header.db_file.write_all(data.as_ref())?;

        Ok(())
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